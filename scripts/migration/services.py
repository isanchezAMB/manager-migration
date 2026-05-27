import csv
import re
import uuid
import oracledb
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text


def _extract_from_oracle(oracle_config):
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT IDSERVEI_PO, CODI, NOM, EMAIL FROM PIO_SERVEI_PO WHERE CODI IS NOT NULL")
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
    return pd.DataFrame(rows, columns=columns)


def _extract_from_csv():
    rows = []
    with open("data/oracle_export/PIO_SERVEI_PO.csv", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("CODI", "").strip():
                rows.append({
                    "CODI": row["CODI"].strip(),
                    "NOM": row.get("NOM", ""),
                    "EMAIL": row.get("EMAIL", ""),
                })
    return pd.DataFrame(rows)


def migrate_services(mysql_url, oracle_config=None):
    print("Starting Services migration...")

    # --- Extracción ---
    df = pd.DataFrame()
    if oracle_config:
        try:
            df = _extract_from_oracle(oracle_config)
            print("  Source: Oracle")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")

    if df.empty:
        df = _extract_from_csv()
        print("  Source: CSV")

    if df.empty:
        print("No services found to migrate.")
        return

    # --- Cargar users de MySQL para cruzar por email ---
    engine = create_engine(mysql_url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, email FROM users"))
        users_by_email = {row[1].lower(): row[0] for row in result}

    # --- Transformación ---
    records = []
    for _, row in df.iterrows():
        emails = [
            e.strip().lower()
            for e in re.split(r"[;,\s]+", str(row.get("EMAIL") or ""))
            if "@" in e.strip()
        ]

        owner_id = next((users_by_email[e] for e in emails if e in users_by_email), None)
        manager_id = next((users_by_email[e] for e in emails[1:] if e in users_by_email), None)

        records.append({
            "key": str(row["CODI"]).strip(),
            "initial": str(row["CODI"]).strip(),
            "name": row["NOM"],
            "owner_id": owner_id,
            "manager_id": manager_id,
        })

    # --- Upsert por key (idempotente) ---
    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        existing_keys = {row[0] for row in conn.execute(text("SELECT `key` FROM services"))}

        for r in records:
            if r["key"] in existing_keys:
                conn.execute(text("""
                    UPDATE services
                    SET name = :name, owner_id = :owner_id, manager_id = :manager_id, updated_at = :updated_at
                    WHERE `key` = :key
                """), {**r, "updated_at": now})
                skipped += 1
            else:
                conn.execute(text("""
                    INSERT INTO services (id, `key`, initial, name, owner_id, manager_id, created_at, updated_at)
                    VALUES (:id, :key, :initial, :name, :owner_id, :manager_id, :created_at, :updated_at)
                """), {**r, "id": str(uuid.uuid4()), "created_at": now, "updated_at": now})
                inserted += 1

        conn.commit()

    without_owner = sum(1 for r in records if r["owner_id"] is None)
    print(f"Migration successful: {inserted} inserted, {skipped} updated ({without_owner} without owner).")

    # --- service_user: tots els emails de cada servei ---
    su_inserted = su_skipped = 0
    now = datetime.now()
    with engine.connect() as conn:
        service_map = {r[0]: r[1] for r in conn.execute(text("SELECT `key`, id FROM services"))}
        user_map = {r[0]: r[1] for r in conn.execute(text("SELECT email, id FROM users"))}
        existing_su = {(r[0], r[1]) for r in conn.execute(text("SELECT service_id, user_id FROM service_user"))}

        for _, row in df.iterrows():
            codi = str(row.get("CODI", "")).strip()
            service_id = service_map.get(codi)
            if not service_id:
                continue
            emails = [
                e.strip().lower()
                for e in re.split(r"[;,\s]+", str(row.get("EMAIL") or ""))
                if "@" in e.strip()
            ]
            for email in emails:
                user_id = user_map.get(email)
                if not user_id:
                    continue
                if (service_id, user_id) in existing_su:
                    su_skipped += 1
                    continue
                existing_su.add((service_id, user_id))
                conn.execute(text("""
                    INSERT INTO service_user (service_id, user_id, created_at, updated_at)
                    VALUES (:s, :u, :ca, :ua)
                """), {"s": service_id, "u": user_id, "ca": now, "ua": now})
                su_inserted += 1
        conn.commit()

    print(f"  service_user: {su_inserted} inserted, {su_skipped} already existed.")
