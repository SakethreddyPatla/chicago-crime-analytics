from dotenv import load_dotenv
import requests
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os
import time
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError, ReadTimeout

load_dotenv()
print("ACCOUNT  :", os.getenv("SNOWFLAKE_ACCOUNT"))
print("USER     :", os.getenv("SNOWFLAKE_USER"))
print("DATABASE :", os.getenv("SNOWFLAKE_DATABASE"))

SOCRATA_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
APP_TOKEN   = os.getenv("SOCRATA_APP_TOKEN")
SNOWFLAKE_CONFIG = {
    "account"  : os.getenv("SNOWFLAKE_ACCOUNT"),
    "user"     : os.getenv("SNOWFLAKE_USER"),
    "password" : os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database" : os.getenv("SNOWFLAKE_DATABASE"),
    "schema"   : os.getenv("SNOWFLAKE_SCHEMA"),
    "role"     : os.getenv("SNOWFLAKE_ROLE")
}

TABLE_NAME  = "CRIME_INFO"
FETCH_SIZE  = 20000    # rows per API request
LOAD_CHUNK  = 200000   # load to Snowflake every 200k rows


# ─────────────────────────────────────────
# GET LAST LOADED DATE FROM SNOWFLAKE
# ─────────────────────────────────────────
def get_last_loaded_date(conn):
    print("Checking last loaded data in Snowflake...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT MAX(UPDATED_ON)
            FROM CHICAGO_CRIME_DB.RAW.{TABLE_NAME}
        """)
        result = cursor.fetchone()[0]
        if result:
            if isinstance(result, int):
                result = pd.Timestamp(result, unit="ns")
            print(f"  Last loaded date: {result}")
            return result   # no +1 day — use exact timestamp
        else:
            print("  No data found - will do full load")
            return None
    except Exception as e:
        print(f"  Table not found - will do full load ({e})")
        return None
    finally:
        cursor.close()


# ─────────────────────────────────────────
# FETCH SINGLE BATCH WITH RETRY
# ─────────────────────────────────────────
def fetch_batch(offset, where_clause, max_retries=5):
    params = {
        "$limit" : FETCH_SIZE,
        "$offset": offset,
        "$order" : "case_number"
    }
    if where_clause:
        params["$where"] = where_clause

    for attempt in range(max_retries):
        try:
            response = requests.get(
                SOCRATA_URL,
                headers={"X-App-Token": APP_TOKEN},
                params=params,
                timeout=180
            )
            if response.status_code != 200:
                print(f"  Error: API returned status {response.status_code}")
                return None
            return response.json()

        except (ChunkedEncodingError, RequestsConnectionError, ReadTimeout) as e:
            wait = 2 ** attempt
            print(f"  Connection error (attempt {attempt + 1}/{max_retries}): {type(e).__name__}")
            if attempt < max_retries - 1:
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to fetch batch at offset {offset} after {max_retries} retries"
                ) from e


# ─────────────────────────────────────────
# CLEAN AND PREPARE DATAFRAME
# ─────────────────────────────────────────
def prepare_dataframe(records):
    df = pd.DataFrame(records)

    cols_to_drop = [
        'Location', ':@computed_region_6mkv_f3dw', ':@computed_region_vrxf_vc4k',
        ':@computed_region_bdys_3d7i', ':@computed_region_43wa_7qmu',
        ':@computed_region_rpca_8um6', ':@computed_region_d9mm_jgwp',
        ':@computed_region_d3ds_rm58', ':@computed_region_8hcu_yrd4'
    ]
    for col in cols_to_drop:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df.columns = [col.upper() for col in df.columns]

    if "UPDATED_ON" in df.columns:
        df["UPDATED_ON"] = pd.to_datetime(df["UPDATED_ON"], errors="coerce")
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    for col in ["LATITUDE", "LONGITUDE"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ─────────────────────────────────────────
# LOAD CHUNK INTO SNOWFLAKE (FULL LOAD)
# ─────────────────────────────────────────
def load_chunk_full(conn, df, drop_table=False):
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        if drop_table:
            print(f"  Full load - dropping existing {TABLE_NAME}...")
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        print(f"  Loading {len(df)} rows into {TABLE_NAME}...")
        success, num_chunks, num_rows, output = write_pandas(
            conn              = conn,
            df                = df,
            table_name        = TABLE_NAME,
            database          = SNOWFLAKE_CONFIG["database"],
            schema            = SNOWFLAKE_CONFIG["schema"],
            auto_create_table = True,
            overwrite         = False
        )
        if success:
            print(f"  Chunk loaded! ({num_rows} rows in {num_chunks} chunks)")
        else:
            print(f"  Chunk load failed. Output: {output}")
    finally:
        cursor.close()


# ─────────────────────────────────────────
# MERGE CHUNK INTO SNOWFLAKE (INCREMENTAL)
# ─────────────────────────────────────────
def merge_chunk_incremental(conn, df):
    cursor = conn.cursor()
    TEMP_TABLE = f"{TABLE_NAME}_TEMP"
    try:
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        # Step 1: Load into temp table
        cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")
        print(f"  Loading {len(df)} rows into temp table...")
        success, _, num_rows, output = write_pandas(
            conn              = conn,
            df                = df,
            table_name        = TEMP_TABLE,
            database          = SNOWFLAKE_CONFIG["database"],
            schema            = SNOWFLAKE_CONFIG["schema"],
            auto_create_table = True,
            overwrite         = True
        )
        if not success:
            print(f"  Temp load failed: {output}")
            return

        # Step 2: MERGE into main table on CASE_NUMBER
        cols = [c for c in df.columns if c != "CASE_NUMBER"]
        update_clause = ",\n                ".join(
            [f"target.{c} = source.{c}" for c in cols]
        )
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"source.{c}" for c in df.columns])

        merge_sql = f"""
            MERGE INTO {TABLE_NAME} AS target
            USING {TEMP_TABLE} AS source
            ON target.CASE_NUMBER = source.CASE_NUMBER
            WHEN MATCHED AND source.UPDATED_ON > target.UPDATED_ON THEN
                UPDATE SET
                {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """
        print(f"  Merging into {TABLE_NAME}...")
        cursor.execute(merge_sql)

        # Step 3: Drop temp table
        cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")
        print(f"  Merge complete! ({num_rows} rows processed)")

    finally:
        cursor.close()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  CHICAGO CRIMES — DATA LOADER")
    print("=" * 50)

    # ── Step 1: Get last loaded date (quick connection, close immediately) ──
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("  Connected successfully!")
    try:
        last_date = get_last_loaded_date(conn)
        is_full_load = last_date is None
    finally:
        conn.close()
        print("  Snowflake connection closed after date check.\n")

    # ── Step 2: Build where clause ──
    if last_date:
        date_str = last_date.strftime("%Y-%m-%dT%H:%M:%S.000")
        where_clause = f"updated_on >= '{date_str}'"  # >= to avoid missing boundary records
        print(f"Incremental load - fetching records updated on or after {date_str}")
    else:
        where_clause = None
        print("Full load - fetching all records")

    # ── Step 3: Fetch from Socrata and load/merge to Snowflake in chunks ──
    offset       = 0
    total_loaded = 0
    buffer       = []
    first_chunk  = True

    while True:
        print(f"Fetching rows {offset} to {offset + FETCH_SIZE}...")
        batch = fetch_batch(offset, where_clause)

        if batch is None:
            print("API error - stopping fetch.")
            break
        if not batch:
            print("No more records from API.")
            break

        buffer.extend(batch)
        offset += FETCH_SIZE
        print(f"  Buffer: {len(buffer)} rows | Total loaded so far: {total_loaded}")

        if len(buffer) >= LOAD_CHUNK:
            df = prepare_dataframe(buffer)
            print(f"\nReconnecting to Snowflake for chunk...")
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            try:
                if is_full_load:
                    load_chunk_full(conn, df, drop_table=(first_chunk))
                else:
                    merge_chunk_incremental(conn, df)
            finally:
                conn.close()
                print("  Snowflake connection closed.")

            total_loaded += len(df)
            first_chunk = False
            buffer = []
            print(f"  Cumulative rows loaded: {total_loaded}\n")

    # ── Step 4: Load any remaining rows in buffer ──
    if buffer:
        print(f"\nLoading final buffer of {len(buffer)} rows...")
        df = prepare_dataframe(buffer)
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        try:
            if is_full_load:
                load_chunk_full(conn, df, drop_table=(first_chunk))
            else:
                merge_chunk_incremental(conn, df)
        finally:
            conn.close()
            print("  Snowflake connection closed.")
        total_loaded += len(df)

    print(f"\nTotal rows loaded this run: {total_loaded}")
    print("\n" + "=" * 50)
    print("  PIPELINE COMPLETE!")
    print("=" * 50)