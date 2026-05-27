import csv
import uuid
import re
from datetime import datetime
from sqlalchemy import create_engine, text

try:
    import oracledb
    ORACLEDB_AVAILABLE = True
except ImportError:
    ORACLEDB_AVAILABLE = False


def _name_from_email(email):
    username = email.split("@")[0].lower()
    parts = re.split(r"[._-]", username)
    return " ".join(p.capitalize() for p in parts)


def _extract_from_oracle(oracle_config):
    """Extrae usuarios y emails desde Oracle. Devuelve (wfuser_by_email, all_emails)."""
    wfuser_by_email = {}
    all_emails = set()

    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT CONTACTEMAIL, FULLNAME FROM WFUSER WHERE CONTACTEMAIL IS NOT NULL")
        for email, fullname in cursor.fetchall():
            email = email.strip().lower()
            if "@" in email:
                wfuser_by_email[email] = (fullname or "").strip()

        cursor.execute("SELECT EMAIL FROM PIO_SERVEI_PO WHERE EMAIL IS NOT NULL")
        for (raw,) in cursor.fetchall():
            for e in re.split(r"[;,\s]+", raw or ""):
                e = e.strip().lower()
                if "@" in e:
                    all_emails.add(e)

        cursor.close()

    return wfuser_by_email, all_emails


def _extract_from_csv():
    """Extrae usuarios y emails desde los CSVs exportados (fallback)."""
    wfuser_by_email = {}
    all_emails = set()

    with open("data/oracle_export/WFUSER.csv", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            email = row.get("CONTACTEMAIL", "").strip().lower()
            fullname = row.get("FULLNAME", "").strip()
            if email and "@" in email:
                wfuser_by_email[email] = fullname

    with open("data/oracle_export/PIO_SERVEI_PO.csv", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            raw = row.get("EMAIL", "")
            for e in re.split(r"[;,\s]+", raw):
                e = e.strip().lower()
                if "@" in e:
                    all_emails.add(e)

    return wfuser_by_email, all_emails


def migrate_users(mysql_url, oracle_config=None):
    print("Starting Users migration...")

    # --- Extraer datos: Oracle si está disponible, CSV como fallback ---
    wfuser_by_email = {}
    all_emails = set()

    if oracle_config and ORACLEDB_AVAILABLE:
        try:
            wfuser_by_email, all_emails = _extract_from_oracle(oracle_config)
            print("  Source: Oracle")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            wfuser_by_email, all_emails = _extract_from_csv()
    else:
        wfuser_by_email, all_emails = _extract_from_csv()
        print("  Source: CSV")

    if not all_emails:
        print("No emails found to migrate.")
        return

    # --- Construir registros ---
    now = datetime.now()
    # Bcrypt hash de "changeme" (coste 10, generado offline)
    placeholder_password = "$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi"

    records = []
    for email in sorted(all_emails):
        name = wfuser_by_email.get(email) or _name_from_email(email)
        records.append({
            "id": str(uuid.uuid4()),
            "name": name,
            "type": "intern",
            "email": email,
            "password": placeholder_password,
            "timezone": "Europe/Madrid",
            "date_format": "d/m/Y",
            "time_format": "H:i:s",
            "decimals_number": 2,
            "decimals_separator": ",",
            "thousands_separator": ".",
            "default_locale": "ca",
            "is_admin": 0,
            "is_active": 1,
            "created_at": now,
            "updated_at": now,
        })

    # --- Insertar solo si no existe el email ---
    engine = create_engine(mysql_url)
    inserted = 0
    skipped = 0
    with engine.connect() as conn:
        existing = {row[0] for row in conn.execute(text("SELECT email FROM users"))}
        to_insert = [r for r in records if r["email"] not in existing]
        skipped = len(records) - len(to_insert)

        for r in to_insert:
            conn.execute(text("""
                INSERT INTO users
                    (id, name, type, email, password, timezone, date_format, time_format,
                     decimals_number, decimals_separator, thousands_separator, default_locale,
                     is_admin, is_active, created_at, updated_at)
                VALUES
                    (:id, :name, :type, :email, :password, :timezone, :date_format, :time_format,
                     :decimals_number, :decimals_separator, :thousands_separator, :default_locale,
                     :is_admin, :is_active, :created_at, :updated_at)
            """), r)
        conn.commit()
        inserted = len(to_insert)

    print(f"Migration successful: {inserted} users inserted, {skipped} already existed.")
