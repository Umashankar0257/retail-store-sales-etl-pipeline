
import logging
import re
import getpass
import uuid
from pathlib import Path

import pandas as pd

from sqlalchemy import create_engine, text, URL


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

CSV_FILE = (
    BASE_DIR
    / "data"
    / "store_sales_base_initial_v306_28k_v1.0.csv"
)

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "daily_ingestion.log"

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "store_sales_db"
DB_USER = "postgres"

TARGET_SCHEMA = "public"
TARGET_TABLE = "store_sales"

PRIMARY_KEY = "syspk"

DATE_COLUMNS = [
    "order_date",
]

TIMESTAMP_COLUMNS = [
    "created_timestamp",
    "update_timestamp",
]

INTEGER_COLUMNS = [
    "syspk",
    "order_line_item",
    "quantity",
]

NUMERIC_COLUMNS = [
    "unit_price",
    "store_discount",
    "gateway_discount",
    "shipping_cost",
    "tax_amount",
    "amount_paid",
]

# Types for newly arriving columns.
# Change these according to the actual business meaning.
NEW_COLUMN_TYPES = {
    "new_amount": "NUMERIC(12,2)",
    "new_count": "INTEGER",
    "new_date": "DATE",
    "new_description": "TEXT",
}

# If a new column is not listed above, it is added as TEXT.
DEFAULT_NEW_COLUMN_TYPE = "TEXT"

ALLOW_NEW_COLUMNS = True

# Optional schema contract for existing target columns.
#
# Populate this after checking information_schema.columns.
# Example only:
#
# EXPECTED_TARGET_TYPES = {
#     "syspk": "bigint",
#     "order_date": "date",
#     "amount_paid": "numeric",
# }
#
# Do not copy example types without checking your table.


EXPECTED_TARGET_TYPES = {
    "syspk": "bigint",

    "store_code": "character varying",
    "store_type": "character varying",
    "store_state": "character varying",
    "store_city": "character varying",
    "channel_name": "character varying",
    "order_id": "character varying",

    "order_line_item": "integer",
    "order_date": "date",

    "customer_email": "character varying",
    "customer_segment": "character varying",
    "customer_state": "character varying",
    "customer_city": "character varying",
    "customer_pincode": "character varying",

    "product_sku": "character varying",
    "category": "character varying",

    "quantity": "integer",

    "unit_price": "numeric",
    "store_discount": "numeric",
    "gateway_discount": "numeric",
    "shipping_cost": "numeric",
    "tax_amount": "numeric",
    "amount_paid": "numeric",

    "promo_code": "character varying",
    "transaction_type": "character varying",

    "created_by": "character varying",
    "created_timestamp": "timestamp without time zone",
    "updated_by": "character varying",
    "update_timestamp": "timestamp without time zone",
}



# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# 3. SQL IDENTIFIER VALIDATION
# ============================================================

def quote_identifier(identifier):

    if not re.fullmatch(
        r"[a-zA-Z_][a-zA-Z0-9_]*",
        identifier,
    ):
        raise ValueError(
            f"Unsafe SQL identifier: {identifier}"
        )

    return f'"{identifier}"'


# ============================================================
# 4. DATABASE CONNECTION
# ============================================================

def create_db_engine():

    password = getpass.getpass(
        "Enter PostgreSQL password: "
    )

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=password,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
    )

    return engine


# ============================================================
# 5. READ AND VALIDATE SOURCE CSV
# ============================================================

from decimal import Decimal, InvalidOperation
def read_source_file():

    logger.info("Reading source CSV: %s", CSV_FILE)

    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"CSV file not found: {CSV_FILE}"
        )

    # Read key and financial fields as strings first.
    # This avoids unwanted type inference and preserves
    # exact decimal values from the CSV.
    read_dtypes = {
        PRIMARY_KEY: "string",
        "customer_pincode": "string",
    }

    for col in NUMERIC_COLUMNS:
        read_dtypes[col] = "string"

    df = pd.read_csv(
        CSV_FILE,
        dtype=read_dtypes,
    )

    if df.empty:
        raise ValueError(
            "Source CSV is empty. Load cancelled."
        )

    # Normalize column names.
    df.columns = [
        str(col).strip().lower()
        for col in df.columns
    ]

    if df.columns.duplicated().any():
        raise ValueError(
            "Duplicate column names found in CSV."
        )

    for col in df.columns:
        quote_identifier(col)

    if PRIMARY_KEY not in df.columns:
        raise ValueError(
            f"Required key column '{PRIMARY_KEY}' is missing."
        )

    # Validate primary key.
    df[PRIMARY_KEY] = df[PRIMARY_KEY].str.strip()

    if df[PRIMARY_KEY].isna().any():
        raise ValueError("Null syspk values found.")

    if df[PRIMARY_KEY].eq("").any():
        raise ValueError("Blank syspk values found.")

    if df[PRIMARY_KEY].duplicated().any():
        raise ValueError("Duplicate syspk values found.")

    # Convert BIGINT key.
    try:
        key_values = pd.to_numeric(
            df[PRIMARY_KEY],
            errors="raise",
        )

        if (key_values % 1 != 0).any():
            raise ValueError(
                "syspk contains non-integer values."
            )

        if (
            key_values > 9223372036854775807
        ).any() or (
            key_values < -9223372036854775808
        ).any():
            raise ValueError(
                "syspk exceeds PostgreSQL BIGINT range."
            )

        df[PRIMARY_KEY] = key_values.astype("int64")

    except Exception as error:
        raise ValueError(
            f"Invalid BIGINT values in syspk: {error}"
        )

    # Convert other integer columns.
    for col in INTEGER_COLUMNS:

        if col == PRIMARY_KEY or col not in df.columns:
            continue

        values = pd.to_numeric(
            df[col],
            errors="raise",
        )

        non_null_values = values.dropna()

        if (non_null_values % 1 != 0).any():
            raise ValueError(
                f"Non-integer values found in {col}."
            )

        df[col] = values.astype("Int64")

    # Convert numeric financial fields using Decimal.
    for col in NUMERIC_COLUMNS:

        if col not in df.columns:
            continue

        def convert_decimal(value):

            if pd.isna(value) or value == "":
                return None

            try:
                result = Decimal(str(value))

                if not result.is_finite():
                    raise ValueError(
                        f"Non-finite numeric value: {value}"
                    )

                return result

            except (InvalidOperation, ValueError):
                raise ValueError(
                    f"Invalid numeric value '{value}' "
                    f"in column '{col}'."
                )

        df[col] = df[col].map(convert_decimal)

    # Convert date fields.
    for col in DATE_COLUMNS:

        if col in df.columns:

            df[col] = pd.to_datetime(
                df[col],
                errors="raise",
            ).dt.date

    # Convert timestamp fields.
    for col in TIMESTAMP_COLUMNS:

        if col in df.columns:

            df[col] = pd.to_datetime(
                df[col],
                errors="raise",
            )

    # Convert missing values to Python None.
    df = df.astype(object).where(
        pd.notna(df),
        None,
    )

    logger.info(
        "Source validation successful. Rows: %s, Columns: %s",
        len(df),
        len(df.columns),
    )

    return df



# ============================================================
# 6. CHECK TARGET TABLE
# ============================================================

def check_target_table(connection):

    result = connection.execute(
        text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_name = :table_name
            )
        """),
        {
            "schema_name": TARGET_SCHEMA,
            "table_name": TARGET_TABLE,
        },
    ).scalar_one()

    if not result:
        raise ValueError(
            f"Target table {TARGET_SCHEMA}.{TARGET_TABLE} "
            "does not exist. Create it before running ingestion."
        )


# ============================================================
# 7. READ TARGET TABLE SCHEMA
# ============================================================

def get_target_columns(connection):

    result = connection.execute(
        text("""
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                column_default,
                is_identity,
                is_generated
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {
            "schema_name": TARGET_SCHEMA,
            "table_name": TARGET_TABLE,
        },
    )

    columns = {}

    for row in result:

        columns[row.column_name] = {
            "data_type": row.data_type,
            "udt_name": row.udt_name,
            "is_nullable": row.is_nullable,
            "column_default": row.column_default,
            "is_identity": row.is_identity,
            "is_generated": row.is_generated,
        }

    return columns


# ============================================================
# 8. VALIDATE CONFIGURED SCHEMA CONTRACT
# ============================================================

def validate_schema_contract(target_columns):

    for col, expected_type in EXPECTED_TARGET_TYPES.items():

        if col not in target_columns:
            raise ValueError(
                f"Expected target column is missing: {col}"
            )

        actual_type = target_columns[col]["data_type"]

        if actual_type.lower() != expected_type.lower():

            raise ValueError(
                f"Schema mismatch for '{col}'. "
                f"Expected: {expected_type}; "
                f"Actual: {actual_type}. "
                "Review a controlled database migration."
            )

    logger.info("Configured schema contract validated.")


# ============================================================
# 9. DETECT AND ADD NEW COLUMNS
# ============================================================

def get_new_column_type(column_name):

    pg_type = NEW_COLUMN_TYPES.get(
        column_name,
        DEFAULT_NEW_COLUMN_TYPE,
    )

    # Only allow a controlled set of PostgreSQL type syntax.
    # This prevents CSV values from becoming SQL DDL.
    allowed_type_pattern = (
        r"^(TEXT|INTEGER|BIGINT|SMALLINT|BOOLEAN|DATE|"
        r"REAL|DOUBLE PRECISION|"
        r"NUMERIC(?:\(\d{1,3}(?:,\d{1,3})?\))?|"
        r"VARCHAR(?:\(\d{1,4}\))?|"
        r"CHARACTER VARYING(?:\(\d{1,4}\))?|"
        r"TIMESTAMP(?: WITH(?:OUT)? TIME ZONE)?)$"
    )

    if not re.fullmatch(
        allowed_type_pattern,
        pg_type.strip(),
        flags=re.IGNORECASE,
    ):
        raise ValueError(
            f"Unsupported PostgreSQL type configured for "
            f"{column_name}: {pg_type}"
        )

    return pg_type.upper()


def add_new_columns(connection, df, target_columns):

    source_columns = set(df.columns)
    existing_columns = set(target_columns)

    new_columns = sorted(
        source_columns - existing_columns
    )

    if not new_columns:
        logger.info("No new source columns detected.")
        return

    if not ALLOW_NEW_COLUMNS:
        raise ValueError(
            f"New columns found, but auto-add is disabled: "
            f"{new_columns}"
        )

    for col in new_columns:

        pg_type = get_new_column_type(col)

        connection.execute(
            text(
                f'ALTER TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
                f'ADD COLUMN {quote_identifier(col)} {pg_type}'
            )
        )

        logger.info(
            "New target column added: %s | Type: %s",
            col,
            pg_type,
        )


# ============================================================
# 10. CHECK REQUIRED TARGET COLUMNS
# ============================================================

def validate_required_columns(df, target_columns):

    source_columns = set(df.columns)

    missing_required = []

    for col, metadata in target_columns.items():

        # Generated and identity columns may be populated
        # automatically by PostgreSQL.
        if metadata["is_identity"] == "YES":
            continue

        if metadata["is_generated"] != "NEVER":
            continue

        required = (
            metadata["is_nullable"] == "NO"
            and metadata["column_default"] is None
        )

        if required and col not in source_columns:
            missing_required.append(col)

    if missing_required:
        raise ValueError(
            "Source is missing mandatory target columns: "
            f"{missing_required}"
        )

    logger.info("Required target columns validated.")


# ============================================================
# 11. VALIDATE TARGET PRIMARY KEY DATA
# ============================================================

def ensure_unique_key(connection):

    pk = quote_identifier(PRIMARY_KEY)

    duplicate_count = connection.execute(
        text(f"""
            SELECT COUNT(*)
            FROM (
                SELECT {pk}
                FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            ) duplicates
        """)
    ).scalar_one()

    if duplicate_count > 0:
        raise ValueError(
            f"Target has {duplicate_count} duplicate "
            f"{PRIMARY_KEY} groups. Resolve them first."
        )

    null_count = connection.execute(
        text(f"""
            SELECT COUNT(*)
            FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"
            WHERE {pk} IS NULL
        """)
    ).scalar_one()

    if null_count > 0:
        raise ValueError(
            f"Target has {null_count} null {PRIMARY_KEY} values."
        )

    index_name = f"ux_{TARGET_TABLE}_{PRIMARY_KEY}"

    connection.execute(
        text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS '
            f'"{index_name}" '
            f'ON "{TARGET_SCHEMA}"."{TARGET_TABLE}" ({pk})'
        )
    )

    logger.info("Unique key index verified: %s", index_name)


# ============================================================
# 12. CREATE AND LOAD BATCH-SPECIFIC STAGING TABLE
# ============================================================

def load_staging_table(connection, df, target_columns):

    batch_id = uuid.uuid4().hex[:12]

    staging_table = f"stg_store_sales_{batch_id}"

    staging_df = df[
        [
            col for col in df.columns
            if col in target_columns
        ]
    ].copy()

    # Create staging with target column definitions.
    # This lets PostgreSQL validate values against the
    # existing target data types.
    connection.execute(
        text(
            f'CREATE TABLE "{TARGET_SCHEMA}"."{staging_table}" '
            f'(LIKE "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
            f'INCLUDING DEFAULTS)'
        )
    )

    staging_df.to_sql(
        name=staging_table,
        con=connection,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.info(
        "Staging loaded: %s | Rows: %s",
        staging_table,
        len(staging_df),
    )

    return staging_table, staging_df.columns.tolist()


# ============================================================
# 13. UPSERT INTO TARGET TABLE
# ============================================================

def upsert_data(
    connection,
    staging_table,
    common_columns,
):

    target = (
        f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    )

    staging = (
        f'"{TARGET_SCHEMA}"."{staging_table}"'
    )

    quoted_columns = ", ".join(
        quote_identifier(col)
        for col in common_columns
    )

    update_columns = [
        col for col in common_columns
        if col != PRIMARY_KEY
    ]

    if update_columns:

        update_clause = ", ".join(
            f'{quote_identifier(col)} = '
            f'EXCLUDED.{quote_identifier(col)}'
            for col in update_columns
        )

        changed_condition = " OR ".join(
            f'target.{quote_identifier(col)} '
            f'IS DISTINCT FROM '
            f'EXCLUDED.{quote_identifier(col)}'
            for col in update_columns
        )

        conflict_clause = (
            f'ON CONFLICT ({quote_identifier(PRIMARY_KEY)}) '
            f'DO UPDATE SET {update_clause} '
            f'WHERE {changed_condition}'
        )

    else:
        conflict_clause = (
            f'ON CONFLICT ({quote_identifier(PRIMARY_KEY)}) '
            f'DO NOTHING'
        )

    upsert_sql = text(f"""
        INSERT INTO {target} AS target
        ({quoted_columns})

        SELECT {quoted_columns}
        FROM {staging}

        {conflict_clause}
    """)

    result = connection.execute(upsert_sql)

    affected_rows = result.rowcount

    logger.info(
        "Upsert completed. Affected rows: %s",
        affected_rows,
    )

    return affected_rows


# ============================================================
# 14. MAIN DAILY INGESTION
# ============================================================

def run_ingestion():

    engine = None

    try:

        logger.info("=" * 60)
        logger.info("DAILY INGESTION STARTED")
        logger.info("=" * 60)

        # Step 1: Read and validate CSV.
        df = read_source_file()

        # Step 2: Connect to PostgreSQL.
        engine = create_db_engine()

        # Step 3: Start one database transaction.
        with engine.begin() as connection:

            # Step 4: Verify target table.
            check_target_table(connection)

            # Step 5: Read current target schema.
            target_columns = get_target_columns(connection)

            # Step 6: Add new columns when allowed.
            add_new_columns(
                connection,
                df,
                target_columns,
            )

            # Step 7: Refresh schema after adding columns.
            target_columns = get_target_columns(connection)

            # Step 8: Validate expected schema contract.
            validate_schema_contract(target_columns)

            # Step 9: Check mandatory columns.
            validate_required_columns(
                df,
                target_columns,
            )

            # Step 10: Check unique key in target.
            ensure_unique_key(connection)

            # Step 11: Create and load staging table.
            staging_table, common_columns = load_staging_table(
                connection,
                df,
                target_columns,
            )

            try:

                # Step 12: Upsert records.
                affected_rows = upsert_data(
                    connection,
                    staging_table,
                    common_columns,
                )

            finally:

                # Remove staging table before transaction commits.
                connection.execute(
                    text(
                        f'DROP TABLE IF EXISTS '
                        f'"{TARGET_SCHEMA}"."{staging_table}"'
                    )
                )

                logger.info(
                    "Staging table cleaned up: %s",
                    staging_table,
                )

        # Reaching here means the transaction committed.
        logger.info(
            "DAILY INGESTION COMPLETED SUCCESSFULLY"
        )

        print("\nINGESTION SUCCESSFUL")
        print("--------------------")
        print(f"Source rows: {len(df)}")
        print(f"Source columns: {len(df.columns)}")
        print(f"Affected target rows: {affected_rows}")
        print(f"Log file: {LOG_FILE}")

    except Exception as error:

        logger.exception(
            "DAILY INGESTION FAILED: %s",
            error,
        )

        print("\nINGESTION FAILED")
        print("----------------")
        print(str(error))
        print(f"Check log file: {LOG_FILE}")

        raise

    finally:

        if engine is not None:
            engine.dispose()


# ============================================================
# 15. SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_ingestion()
