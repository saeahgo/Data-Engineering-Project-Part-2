import os
import json
import datetime
import logging
import urllib.request
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from concurrent import futures as concurrent_futures

# Set up logging
date = datetime.datetime.now()
log_dir = f'data/publisher/logs/{date.year}/{date.month}'
os.makedirs(log_dir, exist_ok=True)
log_file = f'{log_dir}/publisher_log_{date.year}-{date.month}-{date.day}.log'
error_log_file = f'{log_dir}/publisher_error_log_{date.year}-{date.month}-{date.day}.log'

logging.basicConfig(
    level=logging.DEBUG,  # Set to debug for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file)
    ]
)
error_logger = logging.getLogger('error')
error_logger.addHandler(logging.FileHandler(error_log_file))
error_logger.setLevel(logging.ERROR)

# URL and project configuration
url = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id='
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "pubsubkey.json")
project_id = "trimet-data-pipeline"
topic_id = "bus-data"

# Set up Pub/Sub client
try:
    pubsub_creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
    topic_path = publisher.topic_path(project_id, topic_id)
    logging.info("Pub/Sub client initialized successfully.")
    print("Pub/Sub client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Pub/Sub client: {e}")
    raise

# Read in the bus list
try:
    with open('vehicles.txt', 'r') as cars:
        vehicles = [bus.strip() for bus in cars.readlines()]
    logging.info(f"Vehicle list loaded: {vehicles}")
except FileNotFoundError:
    logging.error("vehicles.txt not found. Please ensure the file exists.")
    raise
except Exception as e:
    logging.error(f"Error reading vehicle list: {e}")
    raise

# Create error directory if needed
error_dir = f'data/error/{date.year}/{date.month}'
os.makedirs(error_dir, exist_ok=True)

# Logging function for errors
def log_error(error, bus_id):
    logging.error(f"Error fetching data for bus {bus_id}: {error}")
    error_file = f'{error_dir}/{date.day}.txt'
    with open(error_file, 'a') as f:
        f.write(bus_id + '\n')

# Main: Retrieve data for each bus
message_count = 0
futures = []

for bus in vehicles:
    logging.info(f'Fetching data for Bus {bus}')
    try:
        req = urllib.request.urlopen(url + bus)
        data = json.loads(req.read())
        
        for message in data:
            data_str = json.dumps(message)
            # Data must be a bytestring
            data_bytes = data_str.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data_bytes)
            futures.append(future)
            message_count += 1  # Count each message
            # logging.debug(f"Message published for bus {bus}: {data_str}")

    except Exception as e:
        log_error(e, bus)

# Wait for all futures to complete
logging.info("Waiting for all messages to be published.")
for future in concurrent_futures.as_completed(futures):
    try:
        future.result()
    except Exception as e:
        logging.error(f"Publishing error: {e}")

logging.info(f"Total Pub/Sub messages published: {message_count}")
print(f"Total Pub/Sub messages published: {message_count}")

