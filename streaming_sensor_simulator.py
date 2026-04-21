import json
import time
import random
import threading
import os
import sys
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

ENDPOINT = "a19jczo2tpx70z-ats.iot.ap-south-1.amazonaws.com"
PORT = 8883

ROOT_CA = r"C:/Users/vishnu uplenchwar/Downloads/sensor/AmazonRootCA1.pem"
PRIVATE_KEY = r"C:/Users/vishnu uplenchwar/Downloads/sensor/b4d5741cc78a2aae4ea7e23ea838657fcda93c2e2a54c9363dab4aca9c6f60fa-private.pem.key"
CERTIFICATE = r"C:/Users/vishnu uplenchwar/Downloads/sensor/b4d5741cc78a2aae4ea7e23ea838657fcda93c2e2a54c9363dab4aca9c6f60fa-certificate.pem.crt"

NUM_SENSORS = 20

print("Validating certificate files...")
for cert_file in [ROOT_CA, PRIVATE_KEY, CERTIFICATE]:
    if not os.path.exists(cert_file):
        print(f"CERTIFICATE NOT FOUND: {cert_file}")
        sys.exit(1)
    print(f"Found: {cert_file}")

client = AWSIoTMQTTClient("stream-simulator")
client.configureEndpoint(ENDPOINT, PORT)
client.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)

print(f"Connecting to AWS IoT at {ENDPOINT}:{PORT}...")
max_retries = 5
for retry in range(max_retries):
    try:
        client.connect()
        print("Connected to AWS IoT!")
        break
    except Exception as e:
        print(f"Connection failed (attempt {retry + 1}/{max_retries}): {type(e).__name__}: {e}")
        if retry < max_retries - 1:
            wait_time = 2 ** retry
            print(f"   Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Failed to connect after {max_retries} attempts. Check:")
            print("   1. Internet connection (ping google.com)")
            print("   2. Certificate files exist and are valid")
            print("   3. AWS endpoint is correct in AWS IoT Core console")
            print("   4. Port 8883 is not blocked by firewall")
            sys.exit(1)

def generate_data(device_id):
    def dirty(min_v, max_v):
        return random.choice([
            round(random.uniform(min_v, max_v), 2),
            None,
            "NaN",
            random.uniform(-100, 200)
        ])

    data = {
        "device_id": device_id,
        "temperature": dirty(20, 40),
        "humidity": dirty(30, 90),
        "soil_moisture": dirty(10, 80),
        "timestamp": int(time.time())
    }

    if random.random() < 0.2:
        data.pop(random.choice(list(data.keys())))

    return data


def sensor_worker(device_id):
    topic = f"agri/sensor/{device_id}/telemetry"

    while True:
        data = generate_data(device_id)

        try:
            client.publish(topic, json.dumps(data), 1)
            print(f"[{device_id}] {data}")
        except Exception as e:
            print(f"[{device_id}] Publish Error: {type(e).__name__}: {e}")
            if not client.is_connected():
                try:
                    client.connect()
                except:
                    pass

        time.sleep(random.uniform(0.2, 1))


for i in range(NUM_SENSORS):
    threading.Thread(target=sensor_worker, args=(f"sensor-{i}",), daemon=True).start()

print("Streaming started...")

while True:
    time.sleep(1)