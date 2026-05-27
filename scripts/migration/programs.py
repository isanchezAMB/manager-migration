import csv
import uuid
import oracledb
from datetime import datetime
from sqlalchemy import create_engine, text


def _extract_tipusfinancament_oracle(oracle_config):
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT FINANCAMENT FROM PIO_TIPUSFINANCAMENT WHERE FINANCAMENT IS NOT NULL")
        rows = [row[0].strip() for row in cursor.fetchall()]
        cursor.close()
    return rows


def _extract_tipusfinancament_csv():
    seen = set()
    rows = []
    with open("data/oracle_export/PIO_TIPUSFINANCAMENT.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            val = row.get("FINANCAMENT", "").strip()
            if val and val not in seen:
                seen.add(val)
                rows.append(val)
    return rows


def _extract_relations_oracle(oracle_config):
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.ACTUACIONS, t.FINANCAMENT
            FROM PIO_FINANCAMENT f
            LEFT JOIN PIO_TIPUSFINANCAMENT t ON f.TIPUSFINANCAMENT = t.IDTIPUSFINANCAMENT
            WHERE f.ACTUACIONS IS NOT NULL AND t.FINANCAMENT IS NOT NULL
        """)
        rows = [(str(int(float(r[0]))), r[1].strip()) for r in cursor.fetchall()]
        cursor.close()
    return rows


def _extract_relations_csv():
    tipusmap = {}
    with open("data/oracle_export/PIO_TIPUSFINANCAMENT.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            tipusmap[str(row.get("IDTIPUSFINANCAMENT", "")).strip()] = row.get("FINANCAMENT", "").strip()

    rows = []
    with open("data/oracle_export/PIO_FINANCAMENT.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            actuacio = str(row.get("ACTUACIONS", "")).split(".")[0].strip()
            tipus_id = str(row.get("TIPUSFINANCAMENT", "")).split(".")[0].strip()
            financament = tipusmap.get(tipus_id, "")
            if actuacio and financament:
                rows.append((actuacio, financament))
    return rows


def migrate_programs(mysql_url, oracle_config=None):
    print("Starting Programs migration...")

    engine = create_engine(mysql_url)

    # --- Catálogo de programs (idempotente por key) ---
    if oracle_config:
        try:
            names = _extract_tipusfinancament_oracle(oracle_config)
            print("  Source: Oracle")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            names = _extract_tipusfinancament_csv()
    else:
        names = _extract_tipusfinancament_csv()
        print("  Source: CSV")

    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        existing_keys = {r[0] for r in conn.execute(text("SELECT `key` FROM programs"))}
        for name in names:
            if name in existing_keys:
                skipped += 1
                continue
            conn.execute(text("""
                INSERT INTO programs (id, `key`, name, created_at, updated_at)
                VALUES (:id, :key, :name, :created_at, :updated_at)
            """), {"id": str(uuid.uuid4()), "key": name, "name": name, "created_at": now, "updated_at": now})
            inserted += 1
        conn.commit()
    print(f"  Programs: {inserted} inserted, {skipped} already existed.")

    # --- Relaciones action_program (idempotente por par) ---
    if oracle_config:
        try:
            relations = _extract_relations_oracle(oracle_config)
        except Exception as e:
            print(f"  Oracle failed for relations ({e}), falling back to CSV")
            relations = _extract_relations_csv()
    else:
        relations = _extract_relations_csv()

    with engine.connect() as conn:
        action_map = {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text("SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"))
        }
        prog_map = {r[0]: r[1] for r in conn.execute(text("SELECT `key`, id FROM programs"))}
        existing_pairs = {
            (r[0], r[1])
            for r in conn.execute(text("SELECT action_id, program_id FROM action_program"))
        }

        inserted = skipped = orphan = 0
        for caseid, financament in relations:
            action_id = action_map.get(caseid)
            program_id = prog_map.get(financament)
            if not action_id or not program_id:
                orphan += 1
                continue
            pair = (action_id, program_id)
            if pair in existing_pairs:
                skipped += 1
                continue
            conn.execute(text("""
                INSERT INTO action_program (action_id, program_id, created_at, updated_at)
                VALUES (:action_id, :program_id, :created_at, :updated_at)
            """), {"action_id": action_id, "program_id": program_id, "created_at": now, "updated_at": now})
            existing_pairs.add(pair)
            inserted += 1

        conn.commit()

    print(f"  Action-Program relations: {inserted} inserted, {skipped} already existed, {orphan} orphans skipped.")
