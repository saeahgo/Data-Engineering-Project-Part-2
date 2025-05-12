# load_to_postgres.py

import psycopg2
import pandas as pd

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip (
                trip_id     INTEGER PRIMARY KEY,
                route_id    INTEGER,
                vehicle_id  INTEGER,
                service_key TEXT,
                direction   TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS breadcrumb (
                tstamp      TIMESTAMP,
                latitude    DOUBLE PRECISION,
                longitude   DOUBLE PRECISION,
                speed       DOUBLE PRECISION,
                trip_id     INTEGER REFERENCES trip(trip_id),
                PRIMARY KEY (tstamp, trip_id)
            );
        """)
    conn.commit()


def load_final_data(output_path):
    df = pd.read_csv(output_path)
    df.columns = df.columns.str.lower()
    df["tstamp"] = pd.to_datetime(df["tstamp"])

    # Prepare trip and breadcrumb DataFrames
    breadcrumb_df = df.rename(columns={
        'gps_latitude': 'latitude',
        'gps_longitude': 'longitude',
        'event_no_trip': 'trip_id'
    })[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']]

    trip_df = df.rename(columns={
        'event_no_trip': 'trip_id'
    })[['trip_id', 'vehicle_id', 'service_key']]

    trip_df['route_id'] = -1
    trip_df['direction'] = '0'

    trip_df = trip_df.drop_duplicates(subset="trip_id")
    breadcrumb_df.drop_duplicates(subset=["tstamp", "trip_id"], inplace=True)
    breadcrumb_df = breadcrumb_df[breadcrumb_df['trip_id'].isin(trip_df['trip_id'])]

    print(breadcrumb_df.head())
    print(breadcrumb_df.isnull().sum())

    if breadcrumb_df.empty:
        print("No breadcrumb data to load.")
        return 0

    # Save temp CSVs
    breadcrumb_csv = "breadcrumb_temp.csv"
    trip_csv = "trip_temp.csv"
    breadcrumb_df.to_csv(breadcrumb_csv, index=False, header=False,
                         columns=["tstamp", "latitude", "longitude", "speed", "trip_id"])
    trip_df.to_csv(trip_csv, index=False, header=False,
                   columns=["trip_id", "route_id", "vehicle_id", "service_key", "direction"])

    # Connect to DB and load data
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="Rhtodk99!",
        host="localhost"
    )
    cur = conn.cursor()
    create_tables(conn)

    # Disable constraints temporarily
    cur.execute("SET session_replication_role = 'replica';")

    # Create temp tables
    cur.execute("""
        CREATE TEMP TABLE tmp_trip (
            trip_id INTEGER,
            route_id INTEGER,
            vehicle_id INTEGER,
            service_key TEXT,
            direction TEXT
        );
    """)
    cur.execute("""
        CREATE TEMP TABLE tmp_breadcrumb (
            tstamp TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            trip_id INTEGER
        );
    """)

    # Load CSVs
    with open(trip_csv, "r") as f:
        cur.copy_expert("COPY tmp_trip (trip_id, route_id, vehicle_id, service_key, direction) FROM STDIN WITH CSV", f)
    with open(breadcrumb_csv, "r") as f:
        cur.copy_expert("COPY tmp_breadcrumb (tstamp, latitude, longitude, speed, trip_id) FROM STDIN WITH CSV", f)

    # Insert with deduplication
    cur.execute("""
        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
        SELECT * FROM tmp_trip
        ON CONFLICT (trip_id) DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
        SELECT * FROM tmp_breadcrumb
        ON CONFLICT (tstamp, trip_id) DO NOTHING;
    """)

    conn.commit()

    # Add indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trip_vehicle ON trip(vehicle_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_breadcrumb_trip ON breadcrumb(trip_id);")
    conn.commit()

    cur.close()
    conn.close()
    print("Data load complete.")
    return 1


def main():
    load_final_data("data/2025/4/21/all_buses_final.csv") #9/all_buses.csv")


if __name__ == "__main__":
    main()
