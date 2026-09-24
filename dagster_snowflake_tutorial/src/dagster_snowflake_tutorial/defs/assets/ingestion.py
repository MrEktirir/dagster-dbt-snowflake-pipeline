import os
import tempfile
import urllib.request

import dagster as dg
from dagster_snowflake import SnowflakeResource


TAXI_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2023-01.parquet"
)


@dg.asset(
    group_name="raw",
    description="Load January 2023 NYC yellow taxi trips into Snowflake",
    kinds={"snowflake", "python"},
)
def taxi_trips(snowflake: SnowflakeResource) -> dg.MaterializeResult:

    # 1. Download the Parquet file to the Dagster worker.
    with tempfile.TemporaryDirectory() as temp_dir:
        local_file = os.path.join(
            temp_dir,
            "yellow_tripdata_2023-01.parquet",
        )

        urllib.request.urlretrieve(TAXI_URL, local_file)

        with snowflake.get_connection() as conn:
            cursor = conn.cursor()

            # 2. Create the target table.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS TAXI_DATA.RAW.TAXI_TRIPS (
                    VendorID              INTEGER,
                    tpep_pickup_datetime  TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count       FLOAT,
                    trip_distance         FLOAT,
                    RatecodeID            FLOAT,
                    store_and_fwd_flag    STRING,
                    PULocationID          INTEGER,
                    DOLocationID          INTEGER,
                    payment_type          INTEGER,
                    fare_amount           FLOAT,
                    extra                 FLOAT,
                    mta_tax               FLOAT,
                    tip_amount            FLOAT,
                    tolls_amount          FLOAT,
                    improvement_surcharge FLOAT,
                    total_amount          FLOAT,
                    congestion_surcharge  FLOAT,
                    airport_fee           FLOAT
                )
            """)

            # 3. Create a temporary Snowflake stage.
            cursor.execute("""
                CREATE OR REPLACE TEMPORARY STAGE TAXI_PARQUET_STAGE
                    FILE_FORMAT = (
                    TYPE = PARQUET
                    USE_LOGICAL_TYPE = TRUE
                    USE_VECTORIZED_SCANNER = TRUE
                    )
                """)

            # 4. Upload the downloaded file to the stage.
            cursor.execute(
                f"PUT 'file://{local_file}' @TAXI_PARQUET_STAGE "
                "AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
            )

            # 5. Replace the current raw-table contents.
            cursor.execute("""
                TRUNCATE TABLE TAXI_DATA.RAW.TAXI_TRIPS
            """)

            # 6. Load the staged Parquet file into the table.
            cursor.execute("""
                COPY INTO TAXI_DATA.RAW.TAXI_TRIPS
                FROM @TAXI_PARQUET_STAGE
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = ABORT_STATEMENT
            """)

            # 7. Verify how many rows were loaded.
            cursor.execute("""
                SELECT COUNT(*)
                FROM TAXI_DATA.RAW.TAXI_TRIPS
            """)

            rows_loaded = cursor.fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(rows_loaded),
            "source_url": dg.MetadataValue.url(TAXI_URL),
        }
    )