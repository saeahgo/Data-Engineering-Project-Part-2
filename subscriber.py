from load_to_postgres import load_final_data

import datetime
import timeit
import os
import json
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from collections import defaultdict, deque
import pandas as pd
import numpy as np
import threading
import signal
import time

# Global variables
df_lock = threading.Lock()
message_buffer = []
last_message_time = datetime.datetime.now()
COUNT = 0
idle_check_interval = 60  # Check idle time every 60 seconds
idle_threshold = 180  # 3 minutes threshold for idle period

# Logging setup
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

# Service account file and subscription info
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "pubsubkey.json")
project_id = "trimet-data-pipeline"
subscription_id = "bus-data-sub"

# Increment message count and log every 10,000 messages
def increment_message_count():
    global COUNT
    COUNT += 1
    if COUNT % 10000 == 0:
        logging.info(f"Received {COUNT} messages.")

# Process and save messages when idle for 3 minutes
def process_and_save():
    global message_buffer
    while True:
        time.sleep(idle_check_interval)
        current_time = datetime.datetime.now()
        time_since_last_message = (current_time - last_message_time).total_seconds()

        if time_since_last_message >= idle_threshold:
            with df_lock:
                if message_buffer:
                    logging.info("Detected 3 minutes of idle time. Running validation and saving data.")
                    process_buffer()
                else:
                    logging.info("Buffer is empty. Skipping validation and save.")

# Process the buffered messages
def process_buffer():
    global message_buffer
    try:
        df = pd.DataFrame(message_buffer)
        message_buffer = []  # Clear the buffer

        if df is None or df.empty:
            logging.warning("DataFrame is None or empty.")
        # Apply data validation and transformation
        validation(df)
        logging.info(f"Initial OPD_DATE values: {df['OPD_DATE'].head()}")
        logging.info(f"Initial ACT_TIME values: {df['ACT_TIME'].head()}")

        logging.info(f"Before add_tstamp_column: {df.shape}")
        df = add_tstamp_column(df) 
        logging.info(f"After add_tstamp_column: {df.shape}")
        df = add_speed_column(df)
        logging.info(f"After add_speed_column: {df.shape}")

        # Remove rows where SPEED > 80
        initial_len = len(df)
        df = df[df["SPEED"] <= 80]
        removed = initial_len - len(df)
        logging.info(f"Removed {removed} rows where SPEED > 80.")

        df = add_service_key_column(df)
        logging.info(f"After add_servicekey_column: {df.shape}")





        # POST VALIDATION
        # Statistical/Distribution assertion: The ACT_TIME entries should be similar to uniform distribution across different times of the day
        if "ACT_TIME" in df.columns:
            bin_counts, _ = np.histogram(df["ACT_TIME"], bins=24, range=(0, 86400))
            mean = np.mean(bin_counts)
            std_dev = np.std(bin_counts)
            if std_dev > 0.2 * mean:
                logging.warning(f"[STATISTICAL ASSERTION WARNING] ACT_TIME distribution skewed. Std Dev = {std_dev:.2f}, Mean = {mean:.2f}")

        # Summary assertion: The total number of EVENT_NO_TRIP entries must match the total number of unique trips in the dataset
        event_trip_list = df["EVENT_NO_TRIP"].tolist()
        transition_count = 1 if event_trip_list else 0
        for i in range(1, len(event_trip_list)):
            if event_trip_list[i] != event_trip_list[i - 1]:
                transition_count += 1

        unique_trips = df["EVENT_NO_TRIP"].nunique()

        if transition_count != unique_trips:
             logging.warning(f"[SUMMARY ASSERTION WARNING] Unexpected trip transition count. Transitions = {transition_count}, Unique = {unique_trips}")







       # Save final output as CSV
        output_path = f'data/subscriber/{date.year}/{date.month}/{date.day}/all_buses.csv'
        df.to_csv(output_path, index=False)
        
        logging.info(f"Final dataset with {len(df)} records written to {output_path}")

        # Save to Postgres or log the processed data
        load_final_data(df)
        logging.info("Data successfully saved to Postgres.")
    except Exception as e:
        logging.error(f"Error processing buffer: {e}")

# Validation function
def validation(df: pd.DataFrame):
    # Existence Assertions
    required_fields = ["EVENT_NO_TRIP", "VEHICLE_ID", "ACT_TIME", "OPD_DATE"]
    for field in required_fields:
        if field not in df.columns or df[field].isnull().any():
            logging.warning(f"[EXISTENCE ASSERTION WARNING] Missing or null {field}")

    # Limit Assertion: ACT_TIME should be between 0 and 97200 (until 3AM on the next day)
    if "ACT_TIME" in df.columns:
        invalid_act_time = df[(df["ACT_TIME"] < 0) | (df["ACT_TIME"] > 97200)]
        for idx in invalid_act_time.index:
            logging.warning(f"[LIMIT ASSERTION WARNING] Invalid ACT_TIME: {df.at[idx, 'ACT_TIME']}")
    
    # Limit Assertion: METERS should be non-negative (>= 0)
    if "METERS" in df.columns:
        invalid_meters = df[df["METERS"] < 0]
        for idx in invalid_meters.index:
            logging.warning(f"[LIMIT ASSERTION WARNING] Invalid METERS: {df.at[idx, 'METERS']}")

    # Limit Assertion: GPS_HDOP should be positive (> 0)
    if "GPS_HDOP" in df.columns:
        invalid_hdop = df[df["GPS_HDOP"] <= 0]
        for idx in invalid_hdop.index:
            logging.warning(f"[LIMIT ASSERTION WARNING] Invalid GPS_HDOP: {df.at[idx, 'GPS_HDOP']}")

    # Intra-Record Assertion: GPS_LATITUDE and GPS_LONGITUDE cannot both be zero 
    zero_gps = df[(df["GPS_LATITUDE"] == 0) & (df["GPS_LONGITUDE"] == 0)]
    for idx in zero_gps.index:
        logging.warning(f"[INTRA RECORD ASSERTION WARNING] Both GPS_LATITUDE and GPS_LONGITUDE are zero in row {idx}")

    # Inter-Record Assertion: For the same EVENT_NO_TRIP, METERS should increase or remain constant
    if "EVENT_NO_TRIP" in df.columns and "METERS" in df.columns:
        for trip_id, group in df.groupby("EVENT_NO_TRIP"):
            meters = group.sort_values("ACT_TIME")["METERS"]
            if not meters.is_monotonic_increasing:
                logging.warning(f"[INTER RECORD ASSERTION WARNING] METERS not monotonic for trip {trip_id}")

    # Referential Integrity Assertion: VEHICLE_ID should be matched across all rows with the same EVENT_NO_TRIP
    if "EVENT_NO_TRIP" in df.columns and "VEHICLE_ID" in df.columns:
        vehicle_mismatch = df.groupby("EVENT_NO_TRIP")["VEHICLE_ID"].nunique()
        for trip_id, count in vehicle_mismatch.items():
            if count > 1:
                logging.warning(f"[REFERENTIAL INTEGRITY ASSERTION WARNING] Multiple VEHICLE_IDs for trip {trip_id}")

# Transformation functions
def add_tstamp_column(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Validate and extract date from OPD_DATE        
        def extract_date(opd_date_str):
            if isinstance(opd_date_str, str) and ':' in opd_date_str:
                try:
                    date_part = opd_date_str.split(':')[0]
                    return datetime.datetime.strptime(date_part, "%d%b%Y").date()
                except Exception:
                    return None
            return None
        
        logging.info("Extracting OPD_DATE...") 
        # Apply extraction and handle missing values
        df['OPD_DATE'] = df['OPD_DATE'].apply(extract_date)
        #if df["OPD_DATE"].isnull().any():
        #logging.info("Filling missing OPD_DATE values using ffill and bfill.")
        df["OPD_DATE"] = df["OPD_DATE"].fillna(method='ffill').fillna(method='bfill')

        # Ensure OPD_DATE is in datetime.date format
        df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], errors='coerce').dt.date
        if df['OPD_DATE'].isnull().any():
            logging.error("Failed to resolve all OPD_DATE issues.")

        # Validate and convert ACT_TIME to timedelta
        df['TIME_DELTA'] = pd.to_timedelta(df['ACT_TIME'], unit='s', errors='coerce')
        if df['TIME_DELTA'].isnull().any():
            logging.warning("Some ACT_TIME values are invalid or missing.")

        # Combine date and time to create full timestamp
        df['TSTAMP'] = pd.to_datetime(df['OPD_DATE'].astype(str)) + df['TIME_DELTA']
        
    except Exception as e:
        logging.error(f"Failed to create tstamp: {e}")
        df["tstamp"] = pd.NaT
        return df
    return df

def add_speed_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by=["TSTAMP", "EVENT_NO_TRIP"])
    df["SPEED"] = 0.0  # default speed

    # Compute speed within each trip
    def compute_speed(group):
        meters = group["METERS"].values
        times = group["ACT_TIME"].values
        speeds = [0.0]
        for i in range(1, len(group)):
            delta_m = meters[i] - meters[i - 1]
            delta_t = times[i] - times[i - 1]
            speed = delta_m / delta_t if delta_t > 0 else 0.0
            speeds.append(speed)
        if len(speeds) > 1:
            speeds[0] = speeds[1]  # set first breadcrumb's speed = second breadcrumb's speed
        group["SPEED"] = speeds
        return group

    df = df.groupby("EVENT_NO_TRIP", group_keys=False).apply(compute_speed)
    return df



def add_service_key_column(df: pd.DataFrame) -> pd.DataFrame:
    # Create the service_key column
    def get_service_key(date):
        # Get day of the week (0=Monday, 1=Tuesday, ..., 6=Sunday)
        day_of_week = date.weekday()  # Monday=0, Sunday=6
        if day_of_week == 5:  # Saturday
            return "Saturday"
        elif day_of_week == 6:  # Sunday
            return "Sunday"
        else:  # Weekday (Monday to Friday)
            return "Weekday"

    # Apply the service key based on TSTAMP
    df["SERVICE_KEY"] = df["TSTAMP"].apply(lambda x: get_service_key(x))

    return df

# Pub/Sub message processing
def process_message(json_obj):
    global message_buffer, last_message_time
    try:
        with df_lock:
            message_buffer.append(json_obj)
            increment_message_count()
        last_message_time = datetime.datetime.now()  # Update the last message time
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        json_obj = json.loads(message.data.decode("utf-8"))
        process_message(json_obj)
        message.ack()
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        message.nack()

# Main function
def main():
    global last_message_time
    # Set up Pub/Sub client
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Start idle monitoring in a separate thread
    threading.Thread(target=process_and_save, daemon=True).start()

    # Subscribe to messages
    flow_control = pubsub_v1.types.FlowControl(max_messages=1000, max_bytes=20 * 1024 * 1024)
    subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)

    logging.info("Subscriber is listening for messages.")


    '''
    try:
        # Keep the main thread running to listen for messages
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Processing remaining messages.")
        with df_lock:
            process_buffer()
    '''
    # Set up signal handling for graceful shutdown on SIGTERM
    def shutdown_signal_handler(signum, frame):
        logging.info("SIGTERM received. Processing remaining messages before shutdown.")
        with df_lock:
            process_buffer()
        logging.info("Graceful shutdown complete.")
        exit(0)  # Exit cleanly

    # Register the SIGTERM signal handler
    signal.signal(signal.SIGTERM, shutdown_signal_handler)

    # Keep the main thread running to listen for messages
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
