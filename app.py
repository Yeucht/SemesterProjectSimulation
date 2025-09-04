import threading
import time
import random
from flask import Flask, request, jsonify
import requests
import sys, traceback


#=========================
#Flask & HTTP
#=========================
app = Flask(__name__)

#=========================
#HTTP (thread-local)
#=========================
_thread_local = threading.local()

def http_session():
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s

def post_with_timeout(url, payload, base_timeout=15):
    #Shorter timeout when stopping
    global STOP_EVENT
    timeout = 3 if (STOP_EVENT and STOP_EVENT.is_set()) else base_timeout
    return http_session().post(url, json=payload, timeout=timeout)

def _thread_excepthook(args):
    #Python 3.8+: capture thread exceptions
    print(f"[THREAD-EXC] name={args.thread.name} exc={args.exc!r}")
    traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback)

#Enable global thread excepthook
setattr(threading, "excepthook", _thread_excepthook)

#=========================
#Concurrency primitives
#=========================
STOP_EVENT = None
CFG_LOCK = threading.RLock()

#Send counters & last-send per meter
SEQ_LOCK = threading.RLock()
METER_SEQ: dict[int, int] = {}     #meter_id -> send count
LAST_SENT: dict[int, float] = {}   #meter_id -> last perf_counter
RUN_LOCK = threading.RLock()

#=========================
#Default configuration
#(same keys as Java SimulationConfig)
#=========================
CONFIG = {
    "dbType": "QUESTDB",

    "clearTablesFlag": False,
    "retentionWindowMillis": 1_000_000_000,

    "meterFlag": True,            #individuals ON/OFF
    "hesFlag": True,              #HES ON/OFF

    #Individuals
    "probeRate": 4,               #probes/hour produced by a SM
    "meterRate": 4,               #sends/hour per SM (<= probeRate)
    "meterRateRandomness": 0.2,   #interval jitter

    #HES
    "hesRate": 2,                 #sends/day for HES (aggregate cadence)
    "hesRateRandomness": 50,      #0..100 (window position)
    "hesSynchronized": False,     #common base (True) vs free placement (False)

    "hesProbeRate": 4,            #probes/hour per SM attached to HES
    "hesMeterRate": 4,            #sends/hour per SM (within HES window)
    "hesMeterRateRandomness": 0.2,#jitter on micro-send size (via ratio)

    "url": "http://sp-service:8080/api/injection/data",

    "nbrSmartMeters": 5000,
    "nbrHES": 400,
    "nbrMetersPerHES": 10,
    "nbrMetersPerHESRandomness": 0.2,

    #mdms (unused)
    "mdmsBatch": False,
    "mdmsBatchSize": 10
}

def conf():
    #Thread-safe read of CONFIG
    with CFG_LOCK:
        return dict(CONFIG)

#=========================
#Build control (epoch + thread)
#=========================
BUILD_LOCK = threading.RLock()
BUILD_EPOCH = 0
BUILD_THREAD = None

def current_epoch():
    with BUILD_LOCK:
        return BUILD_EPOCH

def bump_epoch():
    #Increment global build epoch
    global BUILD_EPOCH
    with BUILD_LOCK:
        BUILD_EPOCH += 1
        return BUILD_EPOCH

def build_job(epoch: int):
    """
    Async job triggered after /config.
    Can be superseded by a newer epoch.
    """
    #Light debounce to absorb bursts
    time.sleep(0.3)
    if epoch != current_epoch():
        return

    cfg = conf()
    POOLS.rebuild(cfg)
    purge_missing_meters()  #cleanup METER_SEQ/LAST_SENT

    #If running, rebuild manager (kill obsolete workers)
    global hes_MGR
    if hes_MGR is not None:
        hes_MGR.rebuild(cfg)

#=========================
#Utils
#=========================
def now_ms() -> int:
    return int(time.time() * 1000)

def rand_signed_bytes(n: int):
    #Signed values in [-128,127]
    return [random.randint(-40, 127) for _ in range(n)]

def auth_digest_hex(n=40):
    #Fake 40-hex digest
    return ''.join(random.choices("0123456789abcdef", k=n))

def next_sequence(meter_id: int) -> int:
    #Monotonic per-meter sequence
    with SEQ_LOCK:
        METER_SEQ[meter_id] = METER_SEQ.get(meter_id, 0) + 1
        return METER_SEQ[meter_id]

def mark_sent(meter_id: int, t_perf: float):
    #Record last send perf time
    with SEQ_LOCK:
        LAST_SENT[meter_id] = t_perf

def elapsed_hours_since_last(meter_id: int, now_perf: float, default_hours: float) -> float:
    #Hours since last send, fallback to default window
    with SEQ_LOCK:
        t0 = LAST_SENT.get(meter_id)
    if t0 is None:
        return max(0.0, default_hours)
    return max(0.0, (now_perf - t0) / 3600.0)

def getc(d, *keys, default=None):
    """
    Read first existing key among *keys* in dict d.
    Example:getc(cfg,"hesRate","hesRate",default=1)
    """
    for k in keys:
        if k in d:
            return d[k]
    return default

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def stochastic_round(x: float) -> int:
    """
    Fractional rounding: floor(x) + Bernoulli(frac(x)).
    Guarantees >= 0.
    """
    if x <= 0:
        return 0
    base = int(x)
    frac = x - base
    return base + (1 if random.random() < frac else 0)

def payload_len_from_interval(probe_rate_per_hour: float, elapsed_hours: float) -> int:
    """
    Payload length = probes produced during elapsed interval,
    stochastically rounded.
    """
    expected = max(0.0, probe_rate_per_hour) * max(0.0, elapsed_hours)
    n = stochastic_round(expected)
    return max(1, n)

def payload_len_from_ratio(probe_rate_per_hour: float, meter_rate_per_hour: float, rate_jitter: float) -> int:
    """
    Micro-send payload length ≈ probes/send ratio.
    Applies multiplicative jitter on meter_rate.
    """
    meter_rate_per_hour = max(0.0001, meter_rate_per_hour)
    if rate_jitter > 0:
        meter_rate_per_hour *= (1 + random.uniform(-rate_jitter, rate_jitter))
        meter_rate_per_hour = max(0.0001, meter_rate_per_hour)
    ratio = probe_rate_per_hour / meter_rate_per_hour
    n = stochastic_round(max(0.0, ratio))
    return max(1, n)

#=========================
#Payload builders (List[DataPacket])
#=========================
def build_metering_data_entry(seq_value: int, payload_len: int):
    return {
        "sequence": seq_value,
        "status": 0,
        "version": 2,
        "address": None,
        #Signed byte array with length=payload_len
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

#=========================
#ID pools (disjoint)
#=========================
class Pools:
    """
    -pool_individual = [1..nbrSmartMeters]
    -HES pools: start at nbrSmartMeters+1, sizes drawn once
    """
    def __init__(self):
        self.pool_individual: list[int] = []
        self.hes_to_meters: dict[int, list[int]] = {}

    def rebuild(self, cfg):
        if not bool(cfg.get("hesFlag", True)):
            self.hes_to_meters = {}
            #Individuals only
            self.pool_individual = list(range(1, int(cfg.get("nbrSmartMeters", 1)) + 1))
            return
        n_indiv = int(cfg.get("nbrSmartMeters", 1))
        n_hes   = int(cfg.get("nbrHES", 1))
        per_hes = int(cfg.get("nbrMetersPerHES", 1))
        rnd     = float(cfg.get("nbrMetersPerHESRandomness", 0.0))

        #Individual pool
        self.pool_individual = list(range(1, n_indiv + 1))

        #Sample sizes per HES (stable until next rebuild)
        sizes = []
        for _ in range(n_hes):
            s = int(round(per_hes * (1 + random.uniform(-rnd, rnd))))
            sizes.append(max(0, s))

        #Contiguous ID allocation for HES
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

#=========================
#HES Worker (1 thread per HES)
#=========================
class hesWorker(threading.Thread):
    def __init__(self, hes_id: int, meters: list[int], base_cfg: dict, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.hes_id = hes_id
        self.meters = meters[:]  #stable IDs for this HES
        self._stop_event = stop_event
        self._set_cfg(base_cfg)
        self.cycle_start = time.perf_counter()
        self._planify_cycle()
        self.next_fire += random.uniform(0, min(0.5, self._cycle_len_s() * 0.01))  #tiny initial jitter
        self._local_stop = threading.Event()  #per-worker stop
        self._parent_stop = stop_event        #global stop

    def _set_cfg(self, base_cfg: dict):
        local = dict(base_cfg)
        self.url = local["url"]
        self.rate_per_day = max(0.0001, float(local.get("hesRate", 1)))
        self.sync = bool(local.get("hesSynchronized", False))
        self.rand_pct = max(0, min(100, int(local.get("hesRateRandomness", 0))))
        self._local_cfg = local  #for batch/sample rules

    def _cycle_len_s(self) -> float:
        return (24.0 / self.rate_per_day) * 3600.0

    def _planify_cycle(self):
        """
        synchronized=False:
          -rand_pct=0 -> evenly spaced offsets
          -rand_pct=100 -> uniform offset in [0,cycle)
          -else -> linear blend base vs uniform
        synchronized=True:
          -rand_pct=0 -> all at cycle start
          -rand_pct=100 -> uniform offset in [0,cycle)
          -else -> scaled uniform around start
        """
        cycle = self._cycle_len_s()
        a = clamp(self.rand_pct / 100.0, 0.0, 1.0)

        if not self.sync:
            #Base offset by HES rank
            rank, n = self._rank_and_count()
            base = 0.0 if n <= 0 else (rank / n) * cycle

            if self.rand_pct <= 0:
                offset = base
            elif self.rand_pct >= 100:
                offset = random.uniform(0.0, cycle)
            else:
                offset = (1.0 - a) * base + a * random.uniform(0.0, cycle)
        else:
            #Synchronized: base at 0
            if self.rand_pct <= 0:
                offset = 0.0
            elif self.rand_pct >= 100:
                offset = random.uniform(0.0, cycle)
            else:
                offset = a * random.uniform(0.0, cycle)

        self.next_fire = self.cycle_start + max(0.0, min(offset, cycle))

    def _sleep_until(self, t_target):
        #Light sleep loop to react to stop quickly
        while not self._should_stop():
            dt = t_target - time.perf_counter()
            if dt <= 0:
                return
            self._parent_stop.wait(min(0.2, dt))

    def _tick_send(self):
        if not self.meters:
            return
        cfg = self._local_cfg
        nowp = time.perf_counter()
        default_hours = self._cycle_len_s() / 3600.0

        hes_probe_rate = float(cfg.get("hesProbeRate", cfg.get("probeRate", 1.0)))
        hes_send_rate = float(cfg.get("hesMeterRate", cfg.get("meterRate", 1.0)))
        hes_send_rate = min(hes_send_rate, hes_probe_rate)
        rate_jit = float(cfg.get("hesMeterRateRandomness", 0.0))

        batch = []
        for mid in self.meters:
            #Aggregation window since last HES push
            eh = elapsed_hours_since_last(mid, nowp, default_hours)

            #Number of micro-sends in window
            n_probes = max(1, stochastic_round(eh * max(0.0001, hes_send_rate)))

            entries = []
            for _ in range(n_probes):
                #Micro-send size from probe/send ratio with jitter
                payload_len = payload_len_from_ratio(hes_probe_rate, hes_send_rate, rate_jit)
                seq = next_sequence(mid)
                entries.append(build_metering_data_entry(seq, payload_len))

            dp = build_datapacket_for_meter(mid, entries)
            batch.append(dp)
            mark_sent(mid, nowp)

        try:
            r = post_with_timeout(self.url, batch, base_timeout=15)
            if not r.ok:
                print(f"[HES#{self.hes_id}] HTTP {r.status_code} {r.text[:200]}")
            else:
                avg_probes = sum(len(dp["meteringData"]) for dp in batch) / len(batch)
                avg_plen = sum(sum(len(e["payload"]) for e in dp["meteringData"]) for dp in batch)
                avg_plen = avg_plen / max(1, sum(len(dp["meteringData"]) for dp in batch))
                print(f"[HES#{self.hes_id}] meters={len(batch)}; ~probes_per_meter={avg_probes:.1f}; ~payload_len={avg_plen:.2f}")
        except Exception as e:
            print(f"[HES#{self.hes_id}] EXC {e}")

    def run(self):
        try:
            while not self._should_stop():
                self._set_cfg(conf())
                self._sleep_until(self.next_fire)
                if self._should_stop():
                    break
                self._tick_send()
                self.cycle_start = time.perf_counter()
                self._planify_cycle()
        except Exception as e:
            print(f"[HES#{self.hes_id}] FATAL: {e}")
            traceback.print_exc()

    #Helpers
    def _rank_and_count(self) -> tuple[int, int]:
        #Get current HES order from POOLS
        try:
            keys = sorted(POOLS.hes_to_meters.keys())
            n = len(keys)
            r = keys.index(self.hes_id) if n > 0 else 0
            return r, n
        except Exception:
            return 0, 1

    def _should_stop(self) -> bool:
        #Local or global stop flag
        return self._local_stop.is_set() or self._parent_stop.is_set()

#=========================
#Individual Producer
#=========================
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

            probe_rate = float(cfg.get("probeRate", 1.0))
            send_rate  = float(cfg.get("meterRate", 1.0))
            send_rate  = min(send_rate, probe_rate)  #enforce send_rate ≤ probe_rate
            jitter     = float(cfg.get("meterRateRandomness", 0.0))
            nbr        = int(cfg.get("nbrSmartMeters", 1))

            #Global pacing: one random SM per tick
            base_interval = 3600.0 / max(0.0001, send_rate * nbr)
            interval = base_interval * (1 + random.uniform(-jitter, jitter))
            interval = max(0.001, interval)

            mid = POOLS.individual_random_id()
            if mid is not None:
                #Payload length = probes produced during this interval across SMs
                elapsed_h = interval / 3600.0
                payload_len = payload_len_from_interval(probe_rate, elapsed_h * nbr)

                seq = next_sequence(mid)
                entry = build_metering_data_entry(seq, payload_len)
                dp = build_datapacket_for_meter(mid, [entry])

                try:
                    r = post_with_timeout(cfg["url"], [dp], base_timeout=10)
                    if not r.ok:
                        print(f"[INDIV] HTTP {r.status_code} {r.text[:200]}")
                    else:
                        print(f"[INDIV] mid={mid}, payload_len={payload_len}, seq={seq}")
                except Exception as e:
                    print(f"[INDIV] EXC {e}")
                mark_sent(mid, time.perf_counter())

            #Poisson-like pacing
            sleep_s = random.expovariate(1.0 / interval)
            self._stop_event.wait(min(max(sleep_s, 0.001), interval * 3))

#=========================
#HES Manager
#=========================
class hesManager:
    def __init__(self, stop_event: threading.Event):
        self.stop_event = stop_event
        self.workers: dict[int, hesWorker] = {}

    def rebuild(self, cfg: dict):
        if not bool(cfg.get("hesFlag", True)):
            #Cleanly stop workers
            for w in self.workers.values():
                w._local_stop.set()
            for w in self.workers.values():
                if w.is_alive():
                    w.join(timeout=2)
            self.workers = {}
            POOLS.hes_to_meters = {}
            return

        #Recompute pools
        POOLS.rebuild(cfg)

        #Stop & remove obsolete workers
        for hid in list(self.workers.keys()):
            if hid not in POOLS.hes_to_meters:
                self.workers[hid]._local_stop.set()
                if self.workers[hid].is_alive():
                    self.workers[hid].join(timeout=2)
                self.workers.pop(hid, None)

        #Create/update workers
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

#Helpers
def _coerce_config_types(d: dict) -> dict:
    """
    Force correct types for known keys.
    Ignore unknown keys (still passed to CONFIG.update).
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

    typed = dict(d)  #copy
    #Booleans
    for k in ["clearTablesFlag", "meterFlag", "hesFlag", "hesSynchronized", "mdmsBatch"]:
        if k in typed: typed[k] = as_bool(typed[k])

    #Ints
    for k in ["probeRate", "meterRate", "hesRate", "hesProbeRate", "hesMeterRate",
              "nbrSmartMeters", "nbrHES", "nbrMetersPerHES", "mdmsBatchSize"]:
        if k in typed: typed[k] = as_int(typed[k])

    #Floats
    for k in ["meterRateRandomness", "hesMeterRateRandomness", "nbrMetersPerHESRandomness"]:
        if k in typed: typed[k] = as_float(typed[k])

    #Percentages
    if "hesRateRandomness" in typed: typed["hesRateRandomness"] = as_int(typed["hesRateRandomness"])

    return typed

def _diff(old: dict, new: dict) -> dict:
    #Key-wise diff of updated fields
    out = {}
    for k, v in new.items():
        ov = old.get(k, None)
        if ov != v:
            out[k] = {"old": ov, "new": v}
    return out

def purge_missing_meters():
    #Drop sequences/timestamps for meters no longer in pools
    with SEQ_LOCK:
        valid = set(POOLS.pool_individual)
        for ms in POOLS.hes_to_meters.values():
            valid.update(ms)
        for d in (METER_SEQ, LAST_SENT):
            for k in list(d.keys()):
                if k not in valid:
                    d.pop(k, None)

#=========================
#Flask endpoints
#=========================
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

    #---- POST (update) ----
    try:
        raw = request.get_json(force=True, silent=False)
        app.logger.info(f"/config POST raw={raw}")

        if raw is None:
            return jsonify({"status": "error", "msg": "Empty JSON body"}), 400

        #Accept {"config":{...}} wrapper
        data = raw.get("config") if isinstance(raw, dict) and "config" in raw else raw
        if not isinstance(data, dict):
            return jsonify({"status": "error", "msg": "JSON must be an object or {\"config\":{...}}"}), 400

        data = _coerce_config_types(data)

        with CFG_LOCK:
            before = dict(CONFIG)
            CONFIG.update(data)
            after = dict(CONFIG)

        applied = _diff(before, data)

        #Bump epoch and launch interruptible build
        epoch = bump_epoch()
        t = threading.Thread(target=build_job, args=(epoch,), daemon=True)
        t.start()

        return jsonify({
            "status": "accepted",
            "buildEpoch": epoch,
            "appliedChanges": applied,
            "config": conf()
        }), 202

    except Exception as e:
        app.logger.exception("Error while updating config")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/build", methods=["POST"])
def build_only():
    cfg = conf()
    POOLS.rebuild(cfg)
    #Prepare manager without starting threads
    return jsonify({
        "status": "built",
        "mappingPreview": {
            str(hid): {"firstMeter": (meters[0] if meters else None), "count": len(meters)}
            for hid, meters in POOLS.hes_to_meters.items()
        },
        "individualPoolSize": len(POOLS.pool_individual)
    })

@app.route("/start", methods=["GET"])
def start_sim():
    global hes_MGR, INDIV, STOP_EVENT
    with RUN_LOCK:
        if getattr(start_sim, "_running", False):
            return jsonify({"status": "already running"})

        STOP_EVENT = threading.Event()
        start_sim._running = True

        cfg = conf()
        if bool(cfg.get("hesFlag", True)):
            hes_MGR = hesManager(STOP_EVENT)
            hes_MGR.rebuild(cfg)
            hes_MGR.start_all()
        else:
            hes_MGR = None

        INDIV = IndividualProducer(STOP_EVENT)
        INDIV.start()

        nbr_hes = len(hes_MGR.workers) if (hes_MGR and getattr(hes_MGR, "workers", None) is not None) else 0

        return jsonify({
            "status": "simulation started",
            "nbrHES": nbr_hes,
            "individualPoolSize": len(POOLS.pool_individual)
        })

@app.route("/stop", methods=["GET"])
def stop_sim():
    global hes_MGR, INDIV, STOP_EVENT
    with RUN_LOCK:
        if not getattr(start_sim, "_running", False):
            return jsonify({"status": "not running"})

        if STOP_EVENT is not None:
            STOP_EVENT.set()

        #Stop all workers
        if hes_MGR is not None:
            for w in hes_MGR.workers.values():
                w._local_stop.set()
            for w in hes_MGR.workers.values():
                if w.is_alive():
                    w.join(timeout=5)

        if INDIV is not None and INDIV.is_alive():
            INDIV.join(timeout=5)

        hes_MGR = None
        INDIV = None
        STOP_EVENT = None
        start_sim._running = False

        return jsonify({"status": "stopping"}), 202

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
