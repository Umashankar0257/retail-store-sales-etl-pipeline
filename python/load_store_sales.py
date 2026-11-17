import logging
import re
import getpass
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation

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

LOG_FILE = LOG_DIR / "load_store_sales.log"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "store_sales_db"
DB_USER = "postgres"

TARGET_SCHEMA = "public"
TARGET_TABLE = "store_sales"

PRIMARY_KEYS = [
    "store_code",
    "order_id",
    "order_date",
    "order_line_item",
]

DATE_COLUMNS = [
    "order_date",
]

TIMESTAMP_COLUMNS = [
    "created_timestamp",
    "update_timestamp",
]

INTEGER_COLUMNS = [
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

NEW_COLUMN_TYPES = {
    "loyalty_tier": "VARCHAR(50)",
    "syspk": "BIGINT",
    "new_amount": "NUMERIC(12,2)",
    "new_count": "INTEGER",
    "new_date": "DATE",
    "new_description": "TEXT",
}

DEFAULT_NEW_COLUMN_TYPE = "TEXT"
ALLOW_NEW_COLUMNS = True
INTERACTIVE_SCHEMA_APPROVAL = True

EXPECTED_TARGET_TYPES = {
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
# 2. LOGGING SETUP
# ============================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# 3. SQL IDENTIFIER & TYPE HELPERS
# ============================================================

def quote_identifier(identifier):
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")
    return f'"{identifier}"'


def get_new_column_type(column_name):
    pg_type = NEW_COLUMN_TYPES.get(column_name, DEFAULT_NEW_COLUMN_TYPE)
    allowed_type_pattern = (
        r"^(TEXT|INTEGER|BIGINT|SMALLINT|BOOLEAN|DATE|"
        r"REAL|DOUBLE PRECISION|"
        r"NUMERIC(?:\(\d{1,3}(?:,\d{1,3})?\))?|"
        r"VARCHAR(?:\(\d{1,4}\))?|"
        r"CHARACTER VARYING(?:\(\d{1,4}\))?|"
        r"TIMESTAMP(?: WITH(?:OUT)? TIME ZONE)?)$"
    )
    if not re.fullmatch(allowed_type_pattern, pg_type.strip(), flags=re.IGNORECASE):
        raise ValueError(f"Unsupported PostgreSQL type configured for {column_name}: {pg_type}")
    return pg_type.upper()


def parse_flexible_dates(series, col_name):
    """Parse DD-MM-YYYY first, then fallback to ISO YYYY-MM-DD or standard formats."""
    s = series.astype("string").str.strip()
    parsed = pd.to_datetime(s, format="%d-%m-%Y", errors="coerce")
    mask = parsed.isna() & s.notna() & (s != "")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(s.loc[mask], format="%Y-%m-%d", errors="coerce")
    mask2 = parsed.isna() & s.notna() & (s != "")
    if mask2.any():
        parsed.loc[mask2] = pd.to_datetime(s.loc[mask2], dayfirst=True, errors="raise")
    return parsed.dt.date


def parse_flexible_timestamps(series, col_name):
    """Parse DD-MM-YYYY HH:MM first, then fallback to ISO formats."""
    s = series.astype("string").str.strip()
    parsed = pd.to_datetime(s, format="%d-%m-%Y %H:%M", errors="coerce")
    mask = parsed.isna() & s.notna() & (s != "")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(s.loc[mask], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    mask2 = parsed.isna() & s.notna() & (s != "")
    if mask2.any():
        parsed.loc[mask2] = pd.to_datetime(s.loc[mask2], dayfirst=True, errors="raise")
    return parsed


# ============================================================
# 4. DATABASE CONNECTION
# ============================================================

def create_db_engine():
    password = getpass.getpass("Enter PostgreSQL password: ")
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=password,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(database_url, pool_pre_ping=True)


# ============================================================
# 5. SCHEMA DRIFT DETECTION & GOVERNANCE
# ============================================================

def reconcile_schema_drift(connection, df, target_columns):
    """
    Compares source CSV columns against target PostgreSQL columns.
    - Never drops existing target columns (preserves historical data).
    - Checks if missing source columns are nullable or have defaults in DB.
    - Prompts operator for confirmation before filling missing columns with NULL or adding new columns.
    """
    source_cols = set(df.columns)
    target_cols = set(target_columns.keys())

    non_auto_target_cols = {
        col for col, meta in target_columns.items()
        if meta["is_identity"] != "YES" and meta["is_generated"] == "NEVER"
    }

    missing_in_source = sorted(non_auto_target_cols - source_cols)
    extra_in_source = sorted(source_cols - target_cols)

    blocking_missing = [
        col for col in missing_in_source
        if target_columns[col]["is_nullable"] == "NO" and target_columns[col]["column_default"] is None
    ]
    if blocking_missing:
        raise ValueError(
            f"CRITICAL SCHEMA ERROR: Mandatory NOT NULL target columns missing from source CSV: {blocking_missing}"
        )

    if missing_in_source or extra_in_source:
        print("\n" + "!" * 65)
        print("SCHEMA DRIFT DETECTED BETWEEN SOURCE CSV AND TARGET TABLE")
        print("!" * 65)
        if missing_in_source:
            print(f"[-] Dropped/Missing in CSV (exists in DB): {missing_in_source}")
            print("    -> Action if approved: Retain DB columns untouched; insert NULL for these rows.")
        if extra_in_source:
            print(f"[+] New/Extra in CSV (not in DB)       : {extra_in_source}")
            print(f"    -> Action if approved: {'ALTER TABLE ADD COLUMN' if ALLOW_NEW_COLUMNS else 'IGNORE extra columns'}")
        print("-" * 65)

        if INTERACTIVE_SCHEMA_APPROVAL:
            while True:
                choice = input("Confirm schema evolution and proceed with ingestion? (yes/no): ").strip().lower()
                if choice in ("yes", "y"):
                    logger.warning(
                        "OPERATOR APPROVED SCHEMA DRIFT | missing_in_source=%s | extra_in_source=%s",
                        missing_in_source, extra_in_source
                    )
                    break
                elif choice in ("no", "n"):
                    logger.error("INGESTION ABORTED BY OPERATOR DUE TO SCHEMA DRIFT.")
                    raise ValueError("Ingestion aborted by operator due to schema drift.")
                else:
                    print("Invalid input. Please enter 'yes' or 'no'.")

        for col in missing_in_source:
            df[col] = None
            logger.info("Padded missing source column '%s' with None (NULL).", col)

        if extra_in_source and ALLOW_NEW_COLUMNS:
            for col in extra_in_source:
                pg_type = get_new_column_type(col)
                connection.execute(
                    text(
                        f'ALTER TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
                        f'ADD COLUMN {quote_identifier(col)} {pg_type}'
                    )
                )
                logger.info("Added new column to target table: %s (%s)", col, pg_type)

    return df


# ============================================================
# 6. DEDUPLICATION & QUARANTINE
# ============================================================

def deduplicate_source_data(df):
    """Keep latest row per composite business key; quarantine older or conflicting duplicates."""
    original_count = len(df)
    comparison_columns = [c for c in df.columns if c != "_source_row_number"]
    duplicate_mask = df.duplicated(subset=PRIMARY_KEYS, keep=False)
    duplicate_groups = df.loc[duplicate_mask].copy()
    keep_indices = set(df.index[~duplicate_mask])
    quarantine_parts = []

    if not duplicate_groups.empty:
        if "update_timestamp" not in df.columns:
            raise ValueError("update_timestamp is required to resolve duplicate composite business keys.")

        for key, group in duplicate_groups.groupby(PRIMARY_KEYS, dropna=False, sort=False):
            if group["update_timestamp"].isna().any():
                raise ValueError(f"NULL update_timestamp for duplicate business key {key}; cannot select latest row.")

            latest_ts = group["update_timestamp"].max()
            latest = group[group["update_timestamp"] == latest_ts]

            if len(latest[comparison_columns].drop_duplicates()) > 1:
                conflict = group.copy()
                conflict["quarantine_reason"] = (
                    "Conflicting records share the latest update_timestamp; entire composite-key group withheld for manual review"
                )
                quarantine_parts.append(conflict)
                logger.error("AMBIGUOUS DUPLICATE KEY QUARANTINED | key=%s | rows=%s", key, len(group))
                continue

            winner_index = latest.index[0]
            keep_indices.add(winner_index)
            discarded = group.drop(index=winner_index).copy()
            if not discarded.empty:
                discarded["quarantine_reason"] = (
                    f"Duplicate composite key {tuple(PRIMARY_KEYS)}; older update_timestamp quarantined"
                )
                quarantine_parts.append(discarded)

    clean = df.loc[sorted(keep_indices)].copy().sort_index()
    quarantine = (
        pd.concat(quarantine_parts, ignore_index=True)
        if quarantine_parts
        else pd.DataFrame(columns=list(df.columns) + ["quarantine_reason"])
    )

    qfile = None
    if not quarantine.empty:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        qfile = QUARANTINE_DIR / f"initial_load_quarantine_{datetime.now():%Y%m%d_%H%M%S}.csv"
        quarantine.to_csv(qfile, index=False)
        logger.warning("QUARANTINE CREATED | rows=%s | file=%s", len(quarantine), qfile)
    else:
        logger.info("No duplicate business-key rows quarantined.")

    logger.info(
        "SOURCE DEDUP SUMMARY | raw_source_rows=%s | retained_clean=%s | quarantined=%s",
        original_count, len(clean), len(quarantine)
    )
    return clean, quarantine, qfile


# ============================================================
# 7. READ & VALIDATE SOURCE CSV
# ============================================================

def read_source_file():
    logger.info("Reading source CSV: %s", CSV_FILE)
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

    df = pd.read_csv(CSV_FILE, dtype="string")
    if df.empty:
        raise ValueError("Source CSV is empty.")

    df.columns = [str(col).strip().lower() for col in df.columns]
    if df.columns.duplicated().any():
        raise ValueError("Duplicate column names found in source CSV.")
    for col in df.columns:
        quote_identifier(col)

    missing_keys = [c for c in PRIMARY_KEYS if c not in df.columns]
    if missing_keys:
        raise ValueError(f"Composite business key columns missing: {missing_keys}")

    df.insert(0, "_source_row_number", df.index + 2)
    for col in ["store_code", "order_id"]:
        df[col] = df[col].str.strip()

    for col in PRIMARY_KEYS:
        if df[col].isna().any() or df[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"NULL/blank values found in composite business key column: {col}")

    for col in INTEGER_COLUMNS:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="raise")
            if (values.dropna() % 1 != 0).any():
                raise ValueError(f"Non-integer values found in {col}.")
            df[col] = values.astype("Int64")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            def convert_decimal(value):
                if pd.isna(value) or str(value).strip() == "":
                    return None
                try:
                    result = Decimal(str(value).strip())
                    if not result.is_finite():
                        raise ValueError(f"Non-finite numeric value: {value}")
                    return result
                except (InvalidOperation, ValueError):
                    raise ValueError(f"Invalid numeric value '{value}' in column '{col}'.")
            df[col] = df[col].map(convert_decimal)

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = parse_flexible_dates(df[col], col)

    for col in TIMESTAMP_COLUMNS:
        if col in df.columns:
            df[col] = parse_flexible_timestamps(df[col], col)

    clean_df, quarantine_df, quarantine_file = deduplicate_source_data(df)
    clean_df = clean_df.drop(columns=["_source_row_number"], errors="ignore")
    clean_df = clean_df.astype(object).where(pd.notna(clean_df), None)

    return clean_df, quarantine_df, quarantine_file


# ============================================================
# 8. DATABASE & TABLE VALIDATION
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
        {"schema_name": TARGET_SCHEMA, "table_name": TARGET_TABLE},
    ).scalar_one()

    if not result:
        raise ValueError(f"Target table {TARGET_SCHEMA}.{TARGET_TABLE} does not exist. Create it before initial load.")
    logger.info("Target table existence confirmed: %s.%s", TARGET_SCHEMA, TARGET_TABLE)


def ensure_table_empty(connection):
    count = connection.execute(
        text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
    ).scalar_one()

    if count > 0:
        raise RuntimeError(
            f"Initial load aborted: Target table {TARGET_SCHEMA}.{TARGET_TABLE} "
            f"already contains {count} records. Use daily_ingestion.py for incremental upserts."
        )
    logger.info("Target table is empty. Ready for initial load.")


def get_target_columns(connection):
    result = connection.execute(
        text("""
            SELECT column_name, data_type, udt_name, is_nullable, column_default, is_identity, is_generated
            FROM information_schema.columns
            WHERE table_schema = :schema_name AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"schema_name": TARGET_SCHEMA, "table_name": TARGET_TABLE},
    )
    return {
        row.column_name: {
            "data_type": row.data_type,
            "udt_name": row.udt_name,
            "is_nullable": row.is_nullable,
            "column_default": row.column_default,
            "is_identity": row.is_identity,
            "is_generated": row.is_generated,
        }
        for row in result
    }


def validate_schema_contract(target_columns):
    for col, expected_type in EXPECTED_TARGET_TYPES.items():
        if col not in target_columns:
            raise ValueError(f"Expected target column missing in DB: {col}")
        actual_type = target_columns[col]["data_type"]
        if actual_type.lower() != expected_type.lower():
            raise ValueError(
                f"Data type mismatch in DB for '{col}'. Expected: {expected_type}, Actual: {actual_type}"
            )
    logger.info("Target database schema contract validated.")


# ============================================================
# 9. MAIN INITIAL LOAD EXECUTION
# ============================================================

def run_initial_load():
    engine = None
    try:
        logger.info("=" * 80)
        logger.info("INITIAL STORE SALES LOAD STARTED")
        logger.info("=" * 80)

        clean_df, quarantine_df, quarantine_file = read_source_file()
        raw_csv_count = len(clean_df) + len(quarantine_df)

        engine = create_db_engine()

        with engine.begin() as connection:
            check_target_table(connection)
            ensure_table_empty(connection)
            target_columns = get_target_columns(connection)
            validate_schema_contract(target_columns)

            # Reconcile schema drift (missing or extra CSV columns) with operator prompt
            clean_df = reconcile_schema_drift(connection, clean_df, target_columns)
            target_columns = get_target_columns(connection)

            # Restrict DataFrame columns to target table columns
            insert_cols = [c for c in clean_df.columns if c in target_columns]
            insert_df = clean_df[insert_cols].copy()

            logger.info("Inserting %s clean rows into %s.%s...", len(insert_df), TARGET_SCHEMA, TARGET_TABLE)
            insert_df.to_sql(
                name=TARGET_TABLE,
                con=connection,
                schema=TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

            db_count = connection.execute(
                text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
            ).scalar_one()

            # End-to-End Reconciliation Check: CSV Total == DB Inserted + Quarantined
            if raw_csv_count != (db_count + len(quarantine_df)):
                raise ValueError(
                    f"Reconciliation failed! Raw CSV ({raw_csv_count}) != "
                    f"Inserted ({db_count}) + Quarantined ({len(quarantine_df)})."
                )

            if db_count != len(insert_df):
                raise ValueError(
                    f"Row count mismatch: DB count ({db_count}) != Clean rows ({len(insert_df)})."
                )

            logger.info(
                "INITIAL LOAD RECONCILIATION PASSED | CSV Total=%s | Inserted=%s | Quarantined=%s",
                raw_csv_count, db_count, len(quarantine_df)
            )

        print("\nINITIAL LOAD SUCCESSFUL")
        print("-----------------------")
        print(f"Total CSV rows read        : {raw_csv_count}")
        print(f"Inserted into database     : {db_count}")
        print(f"Quarantined duplicates     : {len(quarantine_df)}")
        print(f"Reconciliation Check       : PASSED (CSV = Inserted + Quarantined)")
        if quarantine_file:
            print(f"Quarantine file saved at   : {quarantine_file}")
        print(f"Log file                   : {LOG_FILE}")

    except Exception as error:
        logger.exception("INITIAL STORE SALES LOAD FAILED: %s", error)
        print("\nINITIAL LOAD FAILED")
        print("-------------------")
        print(str(error))
        print(f"Check log file: {LOG_FILE}")
        raise
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    run_initial_load()