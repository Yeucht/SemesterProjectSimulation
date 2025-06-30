import json
import threading
import random
import time
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

config_data = {
    'rate': 3,
    'rate_randomness': 0.8,
    'format': "questdb",
    'url': "http://localhost:8080/api/ingestion",
    'nbr_smart_meters': 5000,
    'batch': False,
    'batch_size': None,
}

running = False
thread = None


def generate_meter_data(smart_meter_id):
    seq = random.randint(1, 1000)
    payload = [random.randint(-128, 127) for _ in range(25)]
    return {
        "authUser": f"M3P{smart_meter_id}",
        "authSerialNumber": f"{smart_meter_id}",
        "authDigest": ''.join(random.choices("0123456789ABCDEF", k=40)),
        "receivedTime": int(time.time() * 1000),
        "connectionCause": 16777216,
        "isAuthenticated": random.choice([True, False]),
        "isMessageBrokerJob": False,
        "archiverConnectionId": None,
        "cacheFileName": None,
        "masterUnitNumber": None,
        "masterUnitOwnerId": None,
        "masterUnitType": None,
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
    while running:
        try:
            rate = config_data.get("rate", 3)
            randomness = config_data.get("rate_randomness", 0.8)
            delay = max(0.01, 1.0 / (rate + random.uniform(-randomness, randomness)))
            meter_id = random.randint(1, config_data.get("nbr_smart_meters", 5000))
            data = generate_meter_data(meter_id)
            url = config_data.get("url")

            # Optional batch handling
            if config_data.get("batch"):
                batch_size = config_data.get("batch_size", 10)
                batch = [generate_meter_data(random.randint(1, config_data.get("nbr_smart_meters"))) for _ in range(batch_size)]
                requests.post(url, json=batch)
            else:
                requests.post(url, json=data)

            time.sleep(delay)

        except Exception as e:
            print(f"[ERROR] {e}")


@app.route('/config', methods=['GET', 'POST'])
def update_config():
    global config_data
    if request.method == 'GET':
        return jsonify({"status": "success", "config": config_data})
    elif request.method == 'POST':
        new_config = request.get_json(force=True)
        config_data.update(new_config)
        return jsonify({"status": "success", "config": config_data})


@app.route('/start', methods=['GET'])
def start_simulation():
    global running, thread
    if not running:
        running = True
        thread = threading.Thread(target=send_data, daemon=True)
        thread.start()
        return jsonify({"status": "simulation started"})
    else:
        return jsonify({"status": "already running"})


@app.route('/stop', methods=['GET'])
def stop_simulation():
    global running
    running = False
    return jsonify({"status": "simulation stopped"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
