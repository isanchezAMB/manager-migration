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



def _extract_fundings_oracle(oracle_config):
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                f.ACTUACIONS,
                t.FINANCAMENT,
                f.IMPORTPREVIST,
                f.PERCPREVISTRESPECTETOTAL,
                f.IMPORTREAL,
                f.PERCREALRESPECTETOTAL
            FROM PIO_FINANCAMENT f
            LEFT JOIN PIO_TIPUSFINANCAMENT t ON f.TIPUSFINANCAMENT = t.IDTIPUSFINANCAMENT
            WHERE f.ACTUACIONS IS NOT NULL
        """)
        rows = cursor.fetchall()
        cursor.close()
    return [
        {
            "caseid": str(int(float(r[0]))),
            "financament": (r[1] or "").strip(),
            "initial_amount": r[2],
            "initial_percentage": r[3],
            "real_amount": r[4],
            "real_percentage": r[5],
        }
        for r in rows
    ]


def _extract_fundings_csv():
    tipusmap = {}
    with open("data/oracle_export/PIO_TIPUSFINANCAMENT.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            tipusmap[str(row.get("IDTIPUSFINANCAMENT", "")).strip()] = row.get("FINANCAMENT", "").strip()

    rows = []
    with open("data/oracle_export/PIO_FINANCAMENT.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            caseid = str(row.get("ACTUACIONS", "")).split(".")[0].strip()
            if not caseid:
                continue
            tipus_id = str(row.get("TIPUSFINANCAMENT", "")).split(".")[0].strip()

            def to_decimal(val):
                try: return float(val) if val.strip() else None
                except: return None

            rows.append({
                "caseid": caseid,
                "financament": tipusmap.get(tipus_id, ""),
                "initial_amount": to_decimal(row.get("IMPORTPREVIST", "")),
                "initial_percentage": to_decimal(row.get("PERCPREVISTRESPECTETOTAL", "")),
                "real_amount": to_decimal(row.get("IMPORTREAL", "")),
                "real_percentage": to_decimal(row.get("PERCREALRESPECTETOTAL", "")),
            })
    return rows


def migrate_fundings(mysql_url, oracle_config=None):
    print("Starting Fundings migration...")

    engine = create_engine(mysql_url)

    # --- Catálogo funding_types (idempotente por key) ---
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
        existing_keys = {r[0] for r in conn.execute(text("SELECT `key` FROM funding_types"))}
        for name in names:
            if name in existing_keys:
                skipped += 1
                continue
            conn.execute(text("""
                INSERT INTO funding_types (id, `key`, initial, description, created_at, updated_at)
                VALUES (:id, :key, :initial, :description, :created_at, :updated_at)
            """), {
                "id": str(uuid.uuid4()), "key": name, "initial": name,
                "description": name, "created_at": now, "updated_at": now,
            })
            inserted += 1
        conn.commit()
    print(f"  Funding types: {inserted} inserted, {skipped} already existed.")

    # --- Fundings (idempotente por action_id + funding_type_id) ---
    if oracle_config:
        try:
            fundings = _extract_fundings_oracle(oracle_config)
        except Exception as e:
            print(f"  Oracle failed for fundings ({e}), falling back to CSV")
            fundings = _extract_fundings_csv()
    else:
        fundings = _extract_fundings_csv()

    with engine.connect() as conn:
        action_map = {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text("SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"))
        }
        ft_map = {r[0]: r[1] for r in conn.execute(text("SELECT `key`, id FROM funding_types"))}
        existing_pairs = {
            (r[0], r[1])
            for r in conn.execute(text("SELECT action_id, funding_type_id FROM fundings WHERE action_id IS NOT NULL AND funding_type_id IS NOT NULL"))
        }

        inserted = skipped = orphan = 0
        for f in fundings:
            action_id = action_map.get(f["caseid"])
            ft_id = ft_map.get(f["financament"])

            if not action_id:
                orphan += 1
                continue

            pair = (action_id, ft_id)
            if pair in existing_pairs:
                skipped += 1
                continue

            conn.execute(text("""
                INSERT INTO fundings
                    (id, action_id, funding_type_id, initial_amount, initial_percentage,
                     real_amount, real_percentage, created_at, updated_at)
                VALUES
                    (:id, :action_id, :funding_type_id, :initial_amount, :initial_percentage,
                     :real_amount, :real_percentage, :created_at, :updated_at)
            """), {
                "id": str(uuid.uuid4()),
                "action_id": action_id,
                "funding_type_id": ft_id,
                "initial_amount": f["initial_amount"],
                "initial_percentage": f["initial_percentage"],
                "real_amount": f["real_amount"],
                "real_percentage": f["real_percentage"],
                "created_at": now,
                "updated_at": now,
            })
            existing_pairs.add(pair)
            inserted += 1

        conn.commit()

    print(f"  Fundings: {inserted} inserted, {skipped} already existed, {orphan} orphans skipped.")
