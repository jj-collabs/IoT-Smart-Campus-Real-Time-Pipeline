import json
import time
import random
import uuid
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os
load_dotenv()
CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME = "iot-hub"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

devices = ["sensor-1", "sensor-2", "sensor-3"]
locations = ["LectureHallA", "ServerRoomB", "Lab1"]

def generate_event():
    event_time = datetime.utcnow()

    # Simulate late events
    if random.random() < 0.1:
        event_time -= timedelta(seconds=random.randint(5, 30))

    device = random.choice(devices)
    location = random.choice(locations)

    # LOCATION-BASED SIMULATION

    if location == "ServerRoomB":

        # Server rooms can overheat but should not overcrowd
        alert_scenario = random.choices(
            ["Normal", "HighTemp"],
            weights=[60, 40],
            k=1
        )[0]

        if alert_scenario == "Normal":
            temperature = round(random.uniform(55, 70), 2)
            occupancy = random.randint(1, 15)

        elif alert_scenario == "HighTemp":
            temperature = round(random.uniform(75, 95), 2)
            occupancy = random.randint(1, 15)

    elif location == "LectureHallA":

        # Lecture halls can overcrowd
        alert_scenario = random.choices(
            ["Normal", "Overcrowded"],
            weights=[50, 50],
            k=1
        )[0]

        if alert_scenario == "Normal":
            temperature = round(random.uniform(55, 70), 2)
            occupancy = random.randint(40, 90)

        elif alert_scenario == "Overcrowded":
            temperature = round(random.uniform(55, 75), 2)
            occupancy = random.randint(131, 160)

    else:  # Lab1

        # Labs mostly normal
        alert_scenario = random.choices(
            ["Normal", "HighTemp"],
            weights=[80, 20],
            k=1
        )[0]

        if alert_scenario == "Normal":
            temperature = round(random.uniform(50, 70), 2)
            occupancy = random.randint(10, 50)

        elif alert_scenario == "HighTemp":
            temperature = round(random.uniform(72, 85), 2)
            occupancy = random.randint(10, 50)

    event = {
        "eventId": str(uuid.uuid4()),
        "deviceId": device,
        "location": location,
        "temperature": temperature,
        "occupancy": occupancy,
        "deviceStatus": random.choice(["active", "inactive"]),
        "eventTime": event_time.isoformat()
    }

    return event

while True:
    batch = producer.create_batch()

    # Simulate burst traffic
    for _ in range(random.randint(1, 20)):
        event = generate_event()

        # Simulate duplicates
        if random.random() < 0.05:
            batch.add(EventData(json.dumps(event)))

        batch.add(EventData(json.dumps(event)))

    producer.send_batch(batch)
    print("Sent batch...")

    time.sleep(random.uniform(0.5, 2))