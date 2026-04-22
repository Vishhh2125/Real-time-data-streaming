import json
import boto3
from datetime import datetime, timedelta

dynamodb = boto3.client('dynamodb')
TABLE_NAME = "IoTProcessedData"

last_values = {}
MAX_AGE = timedelta(minutes=5)


def process_value(device_id, key, value, min_v, max_v, default):
    now = datetime.utcnow()
    source = "original"

    try:
        val = float(value)

        # NaN check
        if val != val:
            raise ValueError

        # Clamp out-of-bound values
        if val < min_v:
            val = min_v
            source = "clamped_low"
        elif val > max_v:
            val = max_v
            source = "clamped_high"

        # Store last valid value
        last_values.setdefault(device_id, {})[key] = {
            "value": val,
            "time": now
        }

        return val, source

    except:
        last = last_values.get(device_id, {}).get(key)

        if last and (now - last["time"] <= MAX_AGE):
            return last["value"], "forward_fill"

        return default, "default"


def save_to_dynamodb(data):
    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item={
            "device_id": {"S": data["device_id"]},
            "processed_at": {"S": data["processed_at"]},

            "temperature": {"N": str(data["temperature"])},
            "temperature_source": {"S": data["temperature_source"]},

            "humidity": {"N": str(data["humidity"])},
            "humidity_source": {"S": data["humidity_source"]},

            "soil_moisture": {"N": str(data["soil_moisture"])},
            "moisture_source": {"S": data["moisture_source"]},

            "is_dry": {"BOOL": data["is_dry"]},
            "comfort_index": {"N": str(data["comfort_index"])}
        }
    )


def lambda_handler(event, context):
    print("RAW EVENT:", json.dumps(event))

    payload = event

    # Extract device id
    device_id = payload.get("device_id") or payload.get("deviceId") or "unknown"

    #  PROCESSING (FULL FIXED)
    temperature, t_src = process_value(
        device_id,
        "temperature",
        payload.get("temperature"),
        0,
        50,
        25
    )

    humidity, h_src = process_value(
        device_id,
        "humidity",
        payload.get("humidity"),
        0,
        100,
        50
    )

    moisture, m_src = process_value(
        device_id,
        "soil_moisture",
        payload.get("soil_moisture") or payload.get("moisture"),
        0,
        100,
        30
    )

    #  FEATURE ENGINEERING
    is_dry = moisture < 30
    comfort_index = (temperature + humidity) / 2

    #  FINAL PROCESSED DATA
    processed = {
        "device_id": device_id,

        "temperature": temperature,
        "temperature_source": t_src,

        "humidity": humidity,
        "humidity_source": h_src,

        "soil_moisture": moisture,
        "moisture_source": m_src,

        "is_dry": is_dry,
        "comfort_index": round(comfort_index, 2),

        "processed_at": datetime.utcnow().isoformat() + "Z"
    }

    print("PROCESSED:", json.dumps(processed))

    #  SAVE TO DB
    save_to_dynamodb(processed)

    return processed  