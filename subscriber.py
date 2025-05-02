import datetime
import timeit
import os
import json
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account

COUNT = 0
event_trip_meters = {}   
all_event_no_trip = []   
event_trip_vehicle = {}  

# Set up logging
date = datetime.datetime.now()
start = timeit.default_timer()
log_dir = f'data/subscriber/logs/{date.year}/{date.month}'
os.makedirs(log_dir, exist_ok=True)
log_file_path = f'{log_dir}/subscriber_log_{date.year}-{date.month}-{date.day}.log'
error_log_file_path = f'{log_dir}/subscriber_error_log_{date.year}-{date.month}-{date.day}.log'

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
error_logger = logging.getLogger('error')
error_logger.addHandler(logging.FileHandler(error_log_file_path))
error_logger.setLevel(logging.ERROR)

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "pubsubkey.json")
project_id = "trimet-data-pipeline"
subscription_id = "bus-data-sub"

# Create directories if they don't exist
if not os.path.exists(f'data/subscriber/{date.year}/{date.month}/{date.day}'): 
    os.makedirs(f'data/subscriber/{date.year}/{date.month}/{date.day}')
if not os.path.exists(f'data/error/{date.year}/{date.month}'): 
    os.makedirs(f'data/error/{date.year}/{date.month}')

# Increment the message counter
def increment_message_count():
    global COUNT
    COUNT += 1
    if COUNT % 10000 == 0:
        logging.info(f"Received {COUNT} messages.")

# Function to log errors when bus data fails to be processed
def log_error(error, bus_id):
    logging.error(f"{error}")
    with open(f'data/error/{date.year}/{date.month}/{date.day}.txt', 'a') as f:
        f.write(bus_id + '\n')

# Assertion Implementation (Part 2 Data Validation) 
def validation(json_obj):
    # Existence Assertions
    required_fields = ["EVENT_NO_TRIP", "VEHICLE_ID", "ACT_TIME"]
    for field in required_fields:
        if field not in json_obj or json_obj[field] is None:
            logging.warning(f"Missing or null {field}")

    # Limit Assertion: ACT_TIME should be between 0 and 83699 (seconds in a day)
    if "ACT_TIME" in json_obj:
        if not (0 <= json_obj["ACT_TIME"] <= 83699):
            logging.warning(f"Invalid ACT_TIME: {json_obj.get('ACT_TIME')}")

    # Limit Assertion: METERS should be non-negative (>= 0)
    if "METERS" in json_obj:
        if json_obj["METERS"] < 0:
            logging.warning(f"Invalid METERS: {json_obj.get('METERS')}")

    # Limit Assertion: GPS_HDOP should be positive (> 0)
    if "GPS_HDOP" in json_obj:
        if json_obj["GPS_HDOP"] <= 0:
            logging.warning(f"Invalid GPS_HDOP: {json_obj.get('GPS_HDOP')}")
    
    # Intra-Record Assertion: GPS_LATITUDE and GPS_LONGITUDE cannot both be zero 
    gps_latitude = json_obj["GPS_LATITUDE"]
    gps_longitude = json_obj["GPS_LONGITUDE"]

    if gps_latitude == 0 and gps_longitude == 0:
        logging.warning(f"Invalid GPS coordinates: both latitude and longitude are zero in record {json_obj}")

    # Inter-Record Assertion: For the same EVENT_NO_TRIP, METERS should increase or remain constant
    event_no_trip = json_obj.get("EVENT_NO_TRIP")
    meters = json_obj.get("METERS")

    if event_no_trip is not None and meters is not None:
        if event_no_trip in event_trip_meters:
            if meters < event_trip_meters[event_no_trip]:
                logging.warning(f"METERS decreased for EVENT_NO_TRIP {event_no_trip}")
        # Update the value for this trip
        event_trip_meters[event_no_trip] = meters
    
    # Collect EVENT_NO_TRIP for summary assertion later
    event_no_trip = json_obj.get("EVENT_NO_TRIP")
    if event_no_trip:
        all_event_no_trip.append(event_no_trip)

    # Referential Integrity Assertion: VEHICLE_ID should be matched across all rows with the same EVENT_NO_TRIP
    vehicle_id = json_obj.get("VEHICLE_ID")
    if event_no_trip is not None and vehicle_id is not None:
        if event_no_trip in event_trip_vehicle:
            if event_trip_vehicle[event_no_trip] != vehicle_id:
                logging.warning(f"[WARNING] VEHICLE_ID mismatch for EVENT_NO_TRIP {event_no_trip}: was {event_trip_vehicle[event_no_trip]}, now {vehicle_id}")
        else:
            event_trip_vehicle[event_no_trip] = vehicle_id


from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

def write_bus_data(json_obj):
    try:
        with open(f'data/subscriber/{date.year}/{date.month}/{date.day}/all_buses.json', 'a') as f:
            json.dump(json_obj, f)
            f.write('\n')
    except Exception as e:
        log_error(e, str(json_obj.get("VEHICLE_ID", "unknown")))

def write_timestamp():
    try:
        with open("/home/saeah/timestamp.txt", "a") as ts_file:
            ts_file.write(f"{datetime.datetime.now().isoformat()}\n")
    except Exception as e:
        log_error(e, "timestamp")

def process_message(json_obj, message):
    try:
        write_bus_data(json_obj)
        write_timestamp()
        validation(json_obj)
        increment_message_count()
        message.ack()
    except Exception as e:
        log_error(e, str(json_obj.get("VEHICLE_ID", "unknown")))
        message.nack()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        json_obj = json.loads(message.data.decode("utf-8"))
        executor.submit(process_message, json_obj, message)
    except Exception as e:
        log_error(e, "message parse error")
        message.nack()

# Main program
def main():
    # Set up Pub/Sub client
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    #flow_control = pubsub_v1.types.FlowControl(max_messages=500)
    flow_control = pubsub_v1.types.FlowControl(
        max_messages=1000,
        max_bytes=20 * 1024 * 1024,  # 20MB
    )

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(
                subscription_path,
                callback=callback,
                flow_control=flow_control
            )
            logging.info(f"Listening for messages on {subscription_path}...")
            streaming_pull_future.result()

        except Exception as e:
            logging.error(f"Subscriber crashed: {e}")
            try:
                streaming_pull_future.cancel()
                streaming_pull_future.result(timeout=10)
            except Exception as cancel_error:
                logging.error(f"Error during cancel: {cancel_error}")
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)

    logging.info(f"Received {COUNT} messages.")
    stop = time.time()
    logging.info(f'Time: {stop - start}')
    print(f"Received {COUNT} messages.")
    print(f"Time: {stop - start}")

if __name__ == "__main__":
    main()
