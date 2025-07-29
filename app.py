import json
import threading
import random
import time
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

config_data = {
    'rate': 3,                  # messages par seconde
    'rate_randomness': 0.8,     # ±80% sur l’intervalle
    'url': "http://sp-service:8080/api/injection/data",
    'nbr_smart_meters': 5000,
    'batch': False,             # False = envoi single, True = envoi en batch
    'batch_size': 10,           # taille de batch de référence
    'batch_randomness': 0.2,    # ±20% sur la taille de batch
    'mdmsBatch': False,
    'mdmsBatchSize': 10,
}

running = False
thread = None


def generate_meter_data(smart_meter_id):
    seq     = random.randint(1, 1000)
    payload = [random.randint(-128, 127) for _ in range(25)]
    return {
        "authUser": f"M3P{smart_meter_id}",
        "authSerialNumber": f"{smart_meter_id}",
        "authDigest": ''.join(random.choices("0123456789ABCDEF", k=40)),
        "receivedTime": int(time.time() * 1000),
        "connectionCause": 16777216,
        "isAuthenticated": random.choice([True, False]),
        "isMessageBrokerJob": False,
        "archiverConnectionId": "null",
        "cacheFileName": "",
        "masterUnitNumber": "null",
        "masterUnitOwnerId": "null",
        "masterUnitType": "null",
        "meteringData": [{
            "sequence": seq,
            "status": 0,
            "version": 2,
            "address": None,
            "payload": payload
        }]
    }


def send_data():
    global running
    rate             = config_data.get("rate", 1)
    rate_rand        = config_data.get("rate_randomness", 0.0)
    meters           = config_data.get("nbr_smart_meters", 1)
    do_batch         = config_data.get("batch", False)
    base_batch_size  = config_data.get("batch_size", 1)
    batch_rand       = config_data.get("batch_randomness", 0.0)
    url              = config_data.get("url")

    interval = 1.0 / rate
    next_time = time.perf_counter()

    while running:
        # Ajustement aléatoire du timing
        adj_interval = interval * (1 + random.uniform(-rate_rand, rate_rand))
        next_time += adj_interval

        # Détermination de la charge à envoyer
        if do_batch:
            # taille de batch variable autour de base_batch_size
            factor = 1 + random.uniform(-batch_rand, batch_rand)
            n = max(1, int(base_batch_size * factor))
            payload = [generate_meter_data(random.randint(1, meters)) for _ in range(n)]
        else:
            payload = generate_meter_data(random.randint(1, meters))

        # Envoi HTTP
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.ok:
                kind = f"batch({len(payload)})" if do_batch else "single"
                print(f"[OK] Sent {kind} payload")
            else:
                print(f"[ERROR] HTTP {resp.status_code} – {resp.text}")
        except Exception as e:
            print(f"[EXC] Error sending data: {e}")

        # Sleep jusqu'au prochain créneau
        sleep_time = next_time - time.perf_counter()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            # Si on est à la bourre, on réajuste sans dormir
            next_time = time.perf_counter()


@app.route('/config', methods=['GET', 'POST'])
def update_config():
    global config_data
    if request.method == 'GET':
        return jsonify({"status": "success", "config": config_data})
    cfg = request.get_json(force=True)
    config_data.update(cfg)
    return jsonify({"status": "success", "config": config_data})


@app.route('/start', methods=['GET'])
def start_simulation():
    global running, thread
    if not running:
        running = True
        thread = threading.Thread(target=send_data, daemon=True)
        thread.start()
        return jsonify({"status": "simulation started"})
    return jsonify({"status": "already running"})


@app.route('/stop', methods=['GET'])
def stop_simulation():
    global running
    running = False
    return jsonify({"status": "simulation stopped"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
