import logging
import re
import getpass
import uuid
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
    / "store_sales_base_initial_v306_28k_v1.1_dropped_column.csv"
)

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "daily_ingestion.log"
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
    "shipping_cost",
    "tax_amount",
    "amount_paid"
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
# 2. LOGGING
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
    Safely handles schema drift without affecting existing database records:
    - Never drops existing target columns (historical values remain intact).
    - Checks if missing CSV columns are nullable or have defaults in PostgreSQL.
    - Prompts operator for confirmation before padding missing columns with NULL or adding new columns.
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
            print("    -> Action if approved: Keep DB columns intact; insert NULL for new/updated rows.")
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
            logger.info(
                "Source column '%s' is missing. "
                "It will be excluded from this batch to preserve existing DB values.",
                col
            )

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
    """Keep latest row per composite business key; quarantine every discarded occurrence."""
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
                    f"Duplicate composite key {tuple(PRIMARY_KEYS)}; latest update_timestamp retained"
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
        qfile = QUARANTINE_DIR / f"store_sales_quarantine_{datetime.now():%Y%m%d_%H%M%S}.csv"
        quarantine.to_csv(qfile, index=False)
        logger.warning("QUARANTINE CREATED | rows=%s | file=%s", len(quarantine), qfile)
    else:
        logger.info("No duplicate business-key rows quarantined.")

    logger.info(
        "SOURCE DEDUP SUMMARY | source_rows=%s | retained_rows=%s | quarantined_rows=%s",
        original_count, len(clean), len(quarantine)
    )
    return clean, quarantine, qfile


# ============================================================
# 7. READ AND VALIDATE SOURCE CSV
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

    logger.info(
        "SOURCE READ COMPLETE | raw_rows=%s | clean_rows=%s | quarantined_rows=%s | quarantine_file=%s",
        len(df), len(clean_df), len(quarantine_df), quarantine_file
    )
    return clean_df, quarantine_df, quarantine_file


# ============================================================
# 8. CHECK & VALIDATE TARGET TABLE SCHEMA
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
        raise ValueError(
            f"Target table {TARGET_SCHEMA}.{TARGET_TABLE} does not exist. Create it before running ingestion."
        )


def get_target_columns(connection):
    result = connection.execute(
        text("""
            SELECT column_name, data_type, udt_name, is_nullable, column_default, is_identity, is_generated
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
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
            raise ValueError(f"Expected target column is missing in DB: {col}")
        actual_type = target_columns[col]["data_type"]
        if actual_type.lower() != expected_type.lower():
            raise ValueError(
                f"Schema mismatch in DB for '{col}'. Expected: {expected_type}; Actual: {actual_type}."
            )
    logger.info("Configured schema contract validated.")


def ensure_unique_key(connection):
    quoted_keys = ", ".join(quote_identifier(col) for col in PRIMARY_KEYS)

    duplicate_count = connection.execute(
        text(f"""
            SELECT COUNT(*)
            FROM (
                SELECT {quoted_keys}
                FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"
                GROUP BY {quoted_keys}
                HAVING COUNT(*) > 1
            ) duplicates
        """)
    ).scalar_one()

    if duplicate_count > 0:
        raise ValueError(f"Target contains {duplicate_count} duplicate business key combinations.")

    null_conditions = " OR ".join(f"{quote_identifier(col)} IS NULL" for col in PRIMARY_KEYS)
    null_count = connection.execute(
        text(f"""
            SELECT COUNT(*)
            FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"
            WHERE {null_conditions}
        """)
    ).scalar_one()

    if null_count > 0:
        raise ValueError(f"Target contains {null_count} records with NULL business key values.")

    index_name = "ux_store_sales_business_key"
    connection.execute(
        text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}"
            ON "{TARGET_SCHEMA}"."{TARGET_TABLE}" ({quoted_keys})
        """)
    )
    logger.info("Composite unique index verified: %s", index_name)


# ============================================================
# 9. STAGING, UPSERT & RECONCILIATION
# ============================================================

def load_staging_table(connection, df, target_columns):
    batch_id = uuid.uuid4().hex[:12]
    staging_table = f"stg_store_sales_{batch_id}"
    staging_df = df[[col for col in df.columns if col in target_columns]].copy()

    connection.execute(
        text(
            f'CREATE TABLE "{TARGET_SCHEMA}"."{staging_table}" '
            f'(LIKE "{TARGET_SCHEMA}"."{TARGET_TABLE}" INCLUDING DEFAULTS)'
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

    logger.info("Staging loaded: %s | Rows: %s", staging_table, len(staging_df))
    return staging_table, staging_df.columns.tolist()


def classify_stage_rows(connection, staging_table, common_columns):
    target = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    staging = f'"{TARGET_SCHEMA}"."{staging_table}"'
    key_join = " AND ".join(f't.{quote_identifier(k)} = s.{quote_identifier(k)}' for k in PRIMARY_KEYS)
    non_keys = [c for c in common_columns if c not in PRIMARY_KEYS]
    changed = " OR ".join(f't.{quote_identifier(c)} IS DISTINCT FROM s.{quote_identifier(c)}' for c in non_keys) or "FALSE"
    row = connection.execute(text(f"""
        SELECT
          COUNT(*) FILTER (WHERE t.{quote_identifier(PRIMARY_KEYS[0])} IS NULL) AS to_insert,
          COUNT(*) FILTER (WHERE t.{quote_identifier(PRIMARY_KEYS[0])} IS NOT NULL AND ({changed})) AS to_update,
          COUNT(*) FILTER (WHERE t.{quote_identifier(PRIMARY_KEYS[0])} IS NOT NULL AND NOT ({changed})) AS unchanged
        FROM {staging} s
        LEFT JOIN {target} t ON {key_join}
    """)).mappings().one()
    counts = {k: int(row[k]) for k in ("to_insert", "to_update", "unchanged")}
    logger.info(
        "PRE-UPSERT CLASSIFICATION | new=%s | changed_existing=%s | unchanged_existing=%s",
        counts["to_insert"], counts["to_update"], counts["unchanged"]
    )
    return counts


def validate_stage_matches_target(connection, staging_table, common_columns):
    target = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    staging = f'"{TARGET_SCHEMA}"."{staging_table}"'
    key_join = " AND ".join(f't.{quote_identifier(k)} = s.{quote_identifier(k)}' for k in PRIMARY_KEYS)
    equal_values = " AND ".join(f't.{quote_identifier(c)} IS NOT DISTINCT FROM s.{quote_identifier(c)}' for c in common_columns)
    total = connection.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar_one()
    matched = connection.execute(text(f"""
        SELECT COUNT(*) FROM {staging} s
        JOIN {target} t ON {key_join}
        WHERE {equal_values}
    """)).scalar_one()
    mismatched = int(total) - int(matched)
    logger.info(
        "SOURCE-TO-TARGET VALIDATION | clean_source_rows=%s | exact_target_matches=%s | mismatches_or_missing=%s",
        total, matched, mismatched
    )
    if mismatched:
        raise ValueError(f"Source-to-target validation failed: {mismatched} clean rows are missing or differ in target.")
    return int(total), int(matched), mismatched


def count_quarantine_target_matches(connection, quarantine_df, target_columns):
    if quarantine_df.empty:
        logger.info("QUARANTINE TARGET CHECK | quarantined_rows=0 | exact_value_matches=0")
        return 0

    usable = [c for c in quarantine_df.columns if c in target_columns]
    qdf = quarantine_df[usable].copy().astype(object).where(pd.notna(quarantine_df[usable]), None)
    qtable = f"stg_quarantine_{uuid.uuid4().hex[:10]}"
    connection.execute(
        text(f'CREATE TABLE "{TARGET_SCHEMA}"."{qtable}" (LIKE "{TARGET_SCHEMA}"."{TARGET_TABLE}" INCLUDING DEFAULTS)')
    )
    try:
        qdf.to_sql(name=qtable, con=connection, schema=TARGET_SCHEMA, if_exists="append", index=False, chunksize=500, method="multi")
        key_join = " AND ".join(f't.{quote_identifier(k)} = q.{quote_identifier(k)}' for k in PRIMARY_KEYS)
        all_equal = " AND ".join(f't.{quote_identifier(c)} IS NOT DISTINCT FROM q.{quote_identifier(c)}' for c in usable)
        exact_matches = connection.execute(text(f"""
            SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{qtable}" q
            JOIN "{TARGET_SCHEMA}"."{TARGET_TABLE}" t ON {key_join}
            WHERE {all_equal}
        """)).scalar_one()
        logger.info(
            "QUARANTINE TARGET CHECK | quarantined_occurrences=%s | exact_value_matches_in_target=%s | not_exactly_present=%s",
            len(qdf), exact_matches, len(qdf) - int(exact_matches)
        )
        return int(exact_matches)
    finally:
        connection.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{qtable}"'))


def upsert_data(connection, staging_table, common_columns):
    target = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    staging = f'"{TARGET_SCHEMA}"."{staging_table}"'
    quoted_columns = ", ".join(quote_identifier(c) for c in common_columns)
    quoted_keys = ", ".join(quote_identifier(c) for c in PRIMARY_KEYS)
    update_columns = [c for c in common_columns if c not in PRIMARY_KEYS]

    if update_columns:
        update_clause = ", ".join(f'{quote_identifier(c)} = EXCLUDED.{quote_identifier(c)}' for c in update_columns)
        changed_condition = " OR ".join(
            f'target.{quote_identifier(c)} IS DISTINCT FROM EXCLUDED.{quote_identifier(c)}' for c in update_columns
        )
        conflict_clause = f'ON CONFLICT ({quoted_keys}) DO UPDATE SET {update_clause} WHERE {changed_condition}'
    else:
        conflict_clause = f'ON CONFLICT ({quoted_keys}) DO NOTHING'

    result = connection.execute(text(f"""
        INSERT INTO {target} AS target ({quoted_columns})
        SELECT {quoted_columns} FROM {staging}
        {conflict_clause}
    """))
    logger.info("UPSERT EXECUTED | PostgreSQL affected_rows=%s", result.rowcount)
    return result.rowcount


# ============================================================
# 10. MAIN DAILY INGESTION WITH RECONCILIATION
# ============================================================

def run_ingestion():
    engine = None
    try:
        logger.info("=" * 80)
        logger.info("DAILY INGESTION STARTED")
        logger.info("COMPOSITE BUSINESS KEY: %s", PRIMARY_KEYS)
        logger.info("SOURCE FILE: %s", CSV_FILE)
        logger.info("TARGET TABLE: %s.%s", TARGET_SCHEMA, TARGET_TABLE)
        logger.info("=" * 80)

        df, quarantine_df, quarantine_file = read_source_file()
        source_raw_count = len(df) + len(quarantine_df)
        logger.info("BATCH COUNTS | raw_source=%s | eligible_clean_source=%s | quarantined=%s",
                    source_raw_count, len(df), len(quarantine_df))

        engine = create_db_engine()
        with engine.begin() as connection:
            check_target_table(connection)
            target_columns = get_target_columns(connection)
            validate_schema_contract(target_columns)

            # Reconcile schema drift (missing or extra CSV columns) with operator prompt
            df = reconcile_schema_drift(connection, df, target_columns)
            target_columns = get_target_columns(connection)

            ensure_unique_key(connection)

            staging_table, common_columns = load_staging_table(connection, df, target_columns)
            try:
                classification = classify_stage_rows(connection, staging_table, common_columns)
                affected_rows = upsert_data(connection, staging_table, common_columns)
                clean_count, matched_count, mismatch_count = validate_stage_matches_target(
                    connection, staging_table, common_columns
                )
                quarantine_exact_matches = count_quarantine_target_matches(connection, quarantine_df, target_columns)

                staged_count = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{staging_table}"')
                ).scalar_one()
                if int(staged_count) != len(df):
                    raise ValueError(f"Staging reconciliation failed: expected {len(df)}, found {staged_count}.")

                logger.info(
                    "FINAL RECONCILIATION | raw_source=%s | clean_source=%s | quarantined=%s | staged=%s | "
                    "inserted=%s | updated=%s | unchanged=%s | postgres_affected=%s | matched=%s",
                    source_raw_count, len(df), len(quarantine_df), staged_count,
                    classification["to_insert"], classification["to_update"], classification["unchanged"],
                    affected_rows, matched_count
                )
            finally:
                connection.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{staging_table}"'))
                logger.info("Staging table cleaned up: %s", staging_table)

        print("\nINGESTION SUCCESSFUL")
        print("--------------------")
        print(f"Raw source rows (clean + quarantined): {source_raw_count}")
        print(f"Clean source rows: {len(df)}")
        print(f"Quarantined rows: {len(quarantine_df)}")
        print(f"Expected inserts: {classification['to_insert']}")
        print(f"Expected updates: {classification['to_update']}")
        print(f"Unchanged existing rows: {classification['unchanged']}")
        print(f"Post-insert exact source/target matches: {matched_count}/{clean_count}")
        print(f"Quarantine exact-value matches in target: {quarantine_exact_matches}")
        print(f"PostgreSQL affected rows: {affected_rows}")
        print(f"Quarantine file: {quarantine_file}")
        print(f"Log file: {LOG_FILE}")

    except Exception as error:
        logger.exception("DAILY INGESTION FAILED: %s", error)
        print("\nINGESTION FAILED")
        print("----------------")
        print(str(error))
        print(f"Check log file: {LOG_FILE}")
        raise
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    run_ingestion()