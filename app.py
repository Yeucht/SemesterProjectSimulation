import threading
import time
import random
from flask import Flask, request, jsonify
import requests

# =========================
# Flask & HTTP
# =========================
app = Flask(__name__)
session = requests.Session()

# =========================
# Concurrency primitives
# =========================
STOP_EVENT = None
CFG_LOCK = threading.RLock()

# Compteurs d'envois & dernier envoi par meter
SEQ_LOCK = threading.RLock()
METER_SEQ: dict[int, int] = {}     # meter_id -> nombre d'envois
LAST_SENT: dict[int, float] = {}   # meter_id -> perf_counter du dernier envoi

# =========================
# Configuration par défaut
# (mêmes clés/orthographe que ta SimulationConfig Java)
# =========================
CONFIG = {
    "dbType": "QUESTDB",

    "clearTablesFlag": False,
    "retentionWindowMillis": 1_000_000_000,

    "meterFlag": True,    # active les smartmeters individuels
    "hesFlag": True,      # active les hes

    "meterRate": 4,                 # probes/heure (individuels)
    "meterRateRandomness": 0.2,      # jitter (0..1) pour individuels

    "hesRate": 2,                   # envois/jour (cadence des hes)
    "hesRateRandomness": 50,        # 0..100 (répartition dans le cycle)
    "hesSynchronized": False,       # base commune + jitter (True) ou répartition + jitter (False)

    "url": "http://sp-service:8080/api/injection/data",

    "nbrSmartMeters": 5000,         # pool individuel = 1..N
    "nbrHES": 400,
    "nbrMetersPerHES": 10,          # moyenne par hes
    "nbrMetersPerHESRandomness": 0.2,  # 0..1 (tirage une fois au rebuild)

    # "physique" de sample par probe (commun)
    "meterPayloadBatch": False,     # si True, chaque probe regroupe plusieurs samples
    "meterPayloadBatchSize": 10,
    "meterPayloadBatchRandomness": 0.2,

    # "physique" des SM rattachés aux hes
    "hesMeterRate": 4,              # probes/heure pour SM des hes
    "hesMeterRateRandomness": 0.2,   # jitter (0..1) appliqué par envoi hes

    # placeholders non utilisés pour l’instant (mdms)
    "mdmsBatch": False,
    "mdmsBatchSize": 10
}

def conf():
    with CFG_LOCK:
        return dict(CONFIG)

# =========================
# Utils
# =========================
def now_ms() -> int:
    return int(time.time() * 1000)

def rand_signed_bytes(n: int):
    # valeurs signées [-128, 127]
    return [random.randint(-128, 127) for _ in range(n)]

def auth_digest_hex(n=40):
    # 40 hex chars (style SHA-256)
    return ''.join(random.choices("0123456789abcdef", k=n))

def next_sequence(meter_id: int) -> int:
    with SEQ_LOCK:
        METER_SEQ[meter_id] = METER_SEQ.get(meter_id, 0) + 1
        return METER_SEQ[meter_id]

def mark_sent(meter_id: int, t_perf: float):
    with SEQ_LOCK:
        LAST_SENT[meter_id] = t_perf

def elapsed_hours_since_last(meter_id: int, now_perf: float, default_hours: float) -> float:
    with SEQ_LOCK:
        t0 = LAST_SENT.get(meter_id)
    if t0 is None:
        # 1er envoi: on agrège un cycle entier par défaut
        return max(0.0, default_hours)
    return max(0.0, (now_perf - t0) / 3600.0)

def payload_len_from_cfg(cfg) -> int:
    """
    Taille de la LISTE 'payload' (array de bytes) dans un MeteringData.
    - meterPayloadBatch=False -> 1
    - meterPayloadBatch=True  -> batchSize +/- randomness
    """
    if not cfg.get("meterPayloadBatch", False):
        return 1
    base = int(cfg.get("meterPayloadBatchSize", 1))
    rnd  = float(cfg.get("meterPayloadBatchRandomness", 0.0))
    n = int(round(base * (1 + random.uniform(-rnd, rnd))))
    return max(1, n)

def getc(d, *keys, default=None):
    """
    Lit la première clé existante parmi *keys* dans le dict d.
    Exemple: getc(cfg, "hesRate","hesRate", default=1)
    """
    for k in keys:
        if k in d:
            return d[k]
    return default


def samples_per_hour(cfg, for_hes: bool) -> float:
    """
    Nombre de samples 'physiques' produits par heure par un SM.
    = (rate) * (batchSize ou 1)
    - Individuel : meterRate
    - hes        : hesMeterRate (+ jitter multiplicatif par envoi via hesMeterRateRandomness)
    """
    if for_hes:
        rate = float(cfg.get("hesMeterRate", cfg.get("meterRate", 1.0)))
        rand = float(cfg.get("hesMeterRateRandomness", 0.0))
        if rand > 0:
            rate *= (1 + random.uniform(-rand, rand))
    else:
        rate = float(cfg.get("meterRate", 1.0))
        # le jitter des individuels est géré par le pacing, pas ici

    if cfg.get("meterPayloadBatch", False):
        return rate * int(cfg.get("meterPayloadBatchSize", 1))
    return rate

# =========================
# Payload builders (toujours List<DataPacket>)
# =========================
def build_metering_data_entry(seq_value: int, payload_len: int):
    return {
        "sequence": seq_value,
        "status": 0,
        "version": 2,
        "address": None,
        # 👇 tableau de bytes (valeurs signées), LONGUEUR = payload_len
        "payload": rand_signed_bytes(payload_len)
    }

def build_datapacket_for_meter(meter_id: int, metering_entries: list[dict]):
    return {
        "authUser": f"M3P{meter_id}",
        "authSerialNumber": f"{meter_id}",
        "authDigest": auth_digest_hex(40),
        "receivedTime": now_ms(),
        "connectionCause": 16777216,
        "isAuthenticated": random.choice([True, False]),
        "isMessageBrokerJob": False,
        "archiverConnectionId": "null",
        "cacheFileName": "",
        "masterUnitNumber": "null",
        "masterUnitOwnerId": "null",
        "masterUnitType": "null",
        "meteringData": metering_entries,
    }




# =========================
# Pools d'IDs (disjoints)
# =========================
class Pools:
    """
    - pool_individual = [1 .. nbrSmartMeters]
    - pool hes: à partir de nbrSmartMeters+1, par hes avec taille tirée une fois
    """
    def __init__(self):
        self.pool_individual: list[int] = []
        self.hes_to_meters: dict[int, list[int]] = {}

    def rebuild(self, cfg):
        if not bool(cfg.get("hesFlag", True)):
            self.hes_to_meters = {}
            # pool individuels seulement
            self.pool_individual = list(range(1, int(cfg.get("nbrSmartMeters", 1)) + 1))
            return
        n_indiv = int(cfg.get("nbrSmartMeters", 1))
        n_hes   = int(cfg.get("nbrHES", 1))
        per_hes = int(cfg.get("nbrMetersPerHES", 1))
        rnd     = float(cfg.get("nbrMetersPerHESRandomness", 0.0))

        # Pool individuels
        self.pool_individual = list(range(1, n_indiv + 1))

        # Tirage des tailles par hes (stable jusqu'au prochain rebuild)
        sizes = []
        for _ in range(n_hes):
            s = int(round(per_hes * (1 + random.uniform(-rnd, rnd))))
            sizes.append(max(0, s))

        # Allocation contiguë d’IDs hes
        start_id = n_indiv + 1
        current  = start_id
        self.hes_to_meters = {}
        for hid, size in enumerate(sizes, start=1):
            if size <= 0:
                self.hes_to_meters[hid] = []
                continue
            ids = list(range(current, current + size))
            self.hes_to_meters[hid] = ids
            current += size

    def individual_random_id(self) -> int | None:
        if not self.pool_individual:
            return None
        return random.choice(self.pool_individual)

POOLS = Pools()

# =========================
# hes Worker (1 thread par hes)
# =========================
class hesWorker(threading.Thread):
    def __init__(self, hes_id: int, meters: list[int], base_cfg: dict, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.hes_id = hes_id
        self.meters = meters[:]  # IDs stables pour ce hes
        self._stop_event = stop_event
        self._set_cfg(base_cfg)
        self.cycle_start = time.perf_counter()
        self._planify_cycle()

    def _set_cfg(self, base_cfg: dict):
        local = dict(base_cfg)
        self.url = local["url"]
        self.rate_per_day = max(0.0001, float(local.get("hesRate", 1)))
        self.sync = bool(local.get("hesSynchronized", False))
        self.rand_pct = max(0, min(100, int(local.get("hesRateRandomness", 0))))
        self._local_cfg = local  # pour batch/sample rules

    def _cycle_len_s(self) -> float:
        return (24.0 / self.rate_per_day) * 3600.0

    def _planify_cycle(self):
        """
        Détermine le moment d’envoi de CE hes dans le cycle courant.
        - sync=True  : base commune (0) + jitter sur toute la fenêtre selon rand_pct
        - sync=False : on place dans la fenêtre avec jitter ; (répartition fine possible, simple ici)
        """
        cycle = self._cycle_len_s()
        if self.rand_pct <= 0:
            offset = 0.0
        elif self.rand_pct >= 100:
            offset = random.uniform(0, cycle)
        else:
            span = cycle
            jitter = (self.rand_pct / 100.0) * span
            # centre milieu de fenêtre ± jitter/2
            offset = max(0.0, (span / 2.0) + random.uniform(-0.5 * jitter, 0.5 * jitter))
        self.next_fire = self.cycle_start + offset

    def _sleep_until(self, t_target):
        while not self._stop_event.is_set():
            dt = t_target - time.perf_counter()
            if dt <= 0:
                return
            self._stop_event.wait(min(0.2, dt))

    def _tick_send(self):
        if not self.meters:
            return
        cfg = self._local_cfg
        nowp = time.perf_counter()
        default_hours = self._cycle_len_s() / 3600.0

        # Taux de PROBES/heure pour les SM sous hes
        base_rate = float(getc(cfg, "hesMeterRate", "hesMeterRate", default=cfg.get("meterRate", 1.0)))
        rate_jit = float(
            getc(cfg, "hesMeterRateRandomness", "hesMeterRateRandomness", "hesMeterRateRandomness", default=0.0))
        if rate_jit > 0:
            base_rate *= (1 + random.uniform(-rate_jit, rate_jit))

        batch = []
        for mid in self.meters:
            # nombre de PROBES écoulés depuis le dernier envoi
            eh = elapsed_hours_since_last(mid, nowp, default_hours)
            n_probes = max(1, int(round(eh * max(0.0001, base_rate))))

            entries = []
            for _ in range(n_probes):
                seq = next_sequence(mid)  # 1 sequence par probe
                payload_len = payload_len_from_cfg(cfg)  # 1 ou batchSize±rnd
                entries.append(build_metering_data_entry(seq, payload_len))

            dp = build_datapacket_for_meter(mid, entries)  # meteringData = n_probes entrées
            batch.append(dp)

            mark_sent(mid, nowp)

        try:
            r = session.post(self.url, json=batch, timeout=15)
            if not r.ok:
                print(f"[HES#{self.hes_id}] HTTP {r.status_code} {r.text[:200]}")
            else:
                print(
                    f"[HES#{self.hes_id}] sent {len(batch)} meters; avg_probes_per_meter≈{(sum(len(dp['meteringData']) for dp in batch) / len(batch)):.1f}")
        except Exception as e:
            print(f"[HES#{self.hes_id}] EXC {e}")

    def run(self):
        while not self._stop_event.is_set():
            # recharger la config (au cas où /config a changé)
            self._set_cfg(conf())
            # attendre le slot
            self._sleep_until(self.next_fire)
            if self._stop_event.is_set():
                break
            # envoyer
            self._tick_send()
            # planifier prochain cycle
            self.cycle_start = time.perf_counter()
            self._planify_cycle()

# =========================
# Producteur Individuels
# =========================
class IndividualProducer(threading.Thread):
    def __init__(self, stop_event: threading.Event):
        super().__init__(daemon=True)
        self._stop_event = stop_event

    def run(self):
        while not self._stop_event.is_set():
            cfg = conf()
            if not cfg.get("meterFlag", True):
                self._stop_event.wait(0.2)
                continue

            rate_per_hour = max(0.0001, float(cfg.get("meterRate", 1)))
            jitter = float(cfg.get("meterRateRandomness", 0.0))
            nbr = cfg.get("nbrSmartMeters", 1)
            base_interval = 3600.0 / (rate_per_hour * nbr)
            interval = base_interval * (1 + random.uniform(-jitter, jitter))

            mid = POOLS.individual_random_id()
            if mid is not None:
                # envoi "live": 1 probe -> n_samples par probe (batchSize si activé)
                payload_len = payload_len_from_cfg(cfg)  # 1 ou batchSize±rnd
                seq = next_sequence(mid)
                entry = build_metering_data_entry(seq, payload_len)
                dp = build_datapacket_for_meter(mid, [entry])  # liste d’1 entrée

                try:
                    r = session.post(cfg["url"], json=[dp], timeout=10)  # toujours LISTE
                    if not r.ok:
                        print(f"[INDIV] HTTP {r.status_code} {r.text[:200]}")
                    else:
                        print(f"[INDIV] sent 1 meter (mid={mid}, payload_len={payload_len}, seq={seq})")
                except Exception as e:
                    print(f"[INDIV] EXC {e}")
                mark_sent(mid, time.perf_counter())

            # pacing de type Poisson pour réalisme
            sleep_s = random.expovariate(1.0 / max(0.001, interval))
            self._stop_event.wait(min(max(sleep_s, 0.001), interval * 3))

# =========================
# hes Manager
# =========================
class hesManager:
    def __init__(self, stop_event: threading.Event):
        self.stop_event = stop_event
        self.workers: dict[int, hesWorker] = {}

    def rebuild(self, cfg: dict):
        if not bool(cfg.get("hesFlag", True)):
            # désactiver proprement
            self.workers = {}
            POOLS.hes_to_meters = {}
            return
        # Recalcule les pools
        POOLS.rebuild(cfg)

        # Purge des workers obsolètes (STOP global gère l’arrêt)
        for hid in list(self.workers.keys()):
            if hid not in POOLS.hes_to_meters:
                self.workers.pop(hid, None)

        # (re)crée/MAJ les workers
        for hid, meters in POOLS.hes_to_meters.items():
            if hid in self.workers:
                self.workers[hid].meters = meters[:]
            else:
                self.workers[hid] = hesWorker(hid, meters, cfg, self.stop_event)

    def start_all(self):
        for w in self.workers.values():
            if not w.is_alive():
                w.start()

hes_MGR = None
INDIV = None


# Helpers
def _coerce_config_types(d: dict) -> dict:
    """
    Force les bons types pour les clés connues.
    Ignore les clés inconnues (on les passe quand même à CONFIG.update).
    """
    def as_bool(x):
        if isinstance(x, bool): return x
        if isinstance(x, str): return x.lower() in ("1","true","t","yes","y","on")
        if isinstance(x, (int, float)): return x != 0
        return False

    def as_int(x):
        if isinstance(x, int): return x
        if isinstance(x, float): return int(x)
        if isinstance(x, str): return int(float(x))
        return x

    def as_float(x):
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str): return float(x)
        return x

    typed = dict(d)  # copie
    # booleans
    for k in ["clearTablesFlag","meterFlag","hesFlag","meterPayloadBatch","hesSynchronized","mdmsBatch"]:
        if k in typed: typed[k] = as_bool(typed[k])
    # ints
    for k in ["meterRate","hesRate","nbrSmartMeters","nbrHES","nbrMetersPerHES",
              "meterPayloadBatchSize","mdmsBatchSize"]:
        if k in typed: typed[k] = as_int(typed[k])
    # floats
    for k in ["meterRateRandomness","nbrMetersPerHESRandomness",
              "meterPayloadBatchRandomness","hesMeterRateRandomness"]:
        if k in typed: typed[k] = as_float(typed[k])
    # hesMeterRate peut être int (préféré), on le force en int
    if "hesMeterRate" in typed: typed["hesMeterRate"] = as_int(typed["hesMeterRate"])
    # hesRateRandomness : pourcentage 0..100
    if "hesRateRandomness" in typed: typed["hesRateRandomness"] = as_int(typed["hesRateRandomness"])
    return typed

def _diff(old: dict, new: dict) -> dict:
    out = {}
    for k, v in new.items():
        ov = old.get(k, None)
        if ov != v:
            out[k] = {"old": ov, "new": v}
    return out



# =========================
# Flask endpoints
# =========================
@app.route("/config", methods=["GET", "POST"])
def api_config():
    global CONFIG, hes_MGR
    if request.method == "GET":
        return jsonify({
            "status": "success",
            "config": conf(),
            "mappingPreview": {
                str(hid): {"firstMeter": (meters[0] if meters else None), "count": len(meters)}
                for hid, meters in POOLS.hes_to_meters.items()
            },
            "individualPool": {
                "first": (POOLS.pool_individual[0] if POOLS.pool_individual else None),
                "count": len(POOLS.pool_individual)
            }
        })

    # ---- POST (update) ----
    try:
        raw = request.get_json(force=True, silent=False)
        app.logger.info(f"/config POST raw={raw}")

        if raw is None:
            return jsonify({"status": "error", "msg": "Empty JSON body"}), 400

        # Certains clients envoient { "config": { ... } }
        data = raw.get("config") if isinstance(raw, dict) and "config" in raw else raw
        if not isinstance(data, dict):
            return jsonify({"status": "error", "msg": "JSON must be an object or {\"config\":{...}}"}), 400

        data = _coerce_config_types(data)

        with CFG_LOCK:
            before = dict(CONFIG)
            CONFIG.update(data)
            after = dict(CONFIG)

        applied = _diff(before, data)

        # Toujours reconstruire les pools selon la nouvelle conf
        POOLS.rebuild(conf())

        # Si la simu tourne, on met à jour les workers existants
        if hes_MGR is not None:
            hes_MGR.rebuild(conf())

        return jsonify({
            "status": "success",
            "appliedChanges": applied,
            "config": conf()
        })

    except Exception as e:
        app.logger.exception("Error while updating config")
        return jsonify({"status": "error", "msg": str(e)}), 500



@app.route("/start", methods=["GET"])
def start_sim():
    global hes_MGR, INDIV, STOP_EVENT
    if getattr(start_sim, "_running", False):
        return jsonify({"status": "already running"})

    # NOUVEL event par run
    STOP_EVENT = threading.Event()
    start_sim._running = True

    # (Re)build pools & manager neuf
    cfg = conf()
    if bool(cfg.get("hesFlag", True)):
        hes_MGR = hesManager(STOP_EVENT)
        hes_MGR.rebuild(cfg)
        hes_MGR.start_all()
    else:
        hes_MGR = None

    # Producteur individuels NEUF
    INDIV = IndividualProducer(STOP_EVENT)
    INDIV.start()

    #Check
    nbr_hes = len(hes_MGR.workers) if (hes_MGR and getattr(hes_MGR, "workers", None) is not None) else 0

    return jsonify({
        "status": "simulation started",
        "nbrHES": nbr_hes,
        "individualPoolSize": len(POOLS.pool_individual)
    })


@app.route("/stop", methods=["GET"])
def stop_sim():
    global hes_MGR, INDIV, STOP_EVENT
    if not getattr(start_sim, "_running", False):
        return jsonify({"status": "not running"})

    # signal d'arrêt
    if STOP_EVENT is not None:
        STOP_EVENT.set()

    # join hes
    if hes_MGR is not None:
        for w in hes_MGR.workers.values():
            if w.is_alive():
                w.join(timeout=2)

    # join individuels
    if INDIV is not None and INDIV.is_alive():
        INDIV.join(timeout=2)

    # reset des refs (IMPORTANT pour pouvoir /start à nouveau)
    hes_MGR = None
    INDIV = None
    STOP_EVENT = None
    start_sim._running = False

    return jsonify({"status": "simulation stopped"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
