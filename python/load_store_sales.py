
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from getpass import getpass
from pathlib import Path

# ==================================================
# 1. CSV FILE LOCATION
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[1]

CSV_PATH = (
    BASE_DIR
    / "data"
    / "store_sales_base_initial_v306_28k_v1.0.csv"
)

# ==================================================
# 2. READ CSV
# ==================================================

print("\nReading CSV file...")

if not CSV_PATH.exists():
    raise FileNotFoundError(
        f"CSV file not found: {CSV_PATH}"
    )

df = pd.read_csv(
    CSV_PATH,
    dtype={"customer_pincode": "string"}
)

print("CSV loaded successfully!")
print("Total records:", len(df))

# ==================================================
# 3. DATA TYPE CONVERSION
# ==================================================

print("\nConverting data types...")

df["order_date"] = pd.to_datetime(
    df["order_date"], errors="raise"
).dt.date

df["created_timestamp"] = pd.to_datetime(
    df["created_timestamp"], errors="raise"
)

df["update_timestamp"] = pd.to_datetime(
    df["update_timestamp"], errors="raise"
)

print("Data type conversion completed!")

# ==================================================
# 4. POSTGRESQL CONNECTION
# ==================================================

print("\nConnecting to PostgreSQL...")

password = getpass("Enter PostgreSQL password: ")

connection_url = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password=password,
    host="localhost",
    port=5432,
    database="store_sales_db"
)

engine = create_engine(connection_url)

# ==================================================
# 5. CHECK DATABASE CONNECTION AND TABLE
# ==================================================

try:

    with engine.connect() as connection:

        print("PostgreSQL connected successfully!")

        existing_count = connection.execute(
            text("SELECT COUNT(*) FROM public.store_sales")
        ).scalar_one()

        print("Existing database records:", existing_count)

    if existing_count > 0:

        raise RuntimeError(
            "Table already contains records. "
            "Upload stopped to prevent duplicates."
        )

    # ==============================================
    # 6. UPLOAD CSV INTO POSTGRESQL
    # ==============================================

    print("\nUploading records...")

    df.to_sql(
        name="store_sales",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )

    print("Upload completed!")

    # ==============================================
    # 7. VALIDATE UPLOAD
    # ==============================================

    print("\nValidating uploaded records...")

    with engine.connect() as connection:

        db_count = connection.execute(
            text("SELECT COUNT(*) FROM public.store_sales")
        ).scalar_one()

    print("\n" + "=" * 45)
    print("          UPLOAD SUMMARY")
    print("=" * 45)

    print("CSV RECORDS :", len(df))
    print("DB RECORDS  :", db_count)

    if len(df) == db_count:
        print("STATUS      : SUCCESS")
        print("Record counts match!")

    else:
        print("STATUS      : WARNING")
        print("Record counts do not match!")

    print("=" * 45)

except Exception as error:

    print("\nERROR OCCURRED:")
    print(type(error).__name__, ":", error)

    raise

finally:

    engine.dispose()
    print("\nDatabase connection closed.")
