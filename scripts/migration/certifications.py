import csv
import json
import uuid
import oracledb
from datetime import datetime
from sqlalchemy import create_engine, text


MONTHS_CA = ["gener", "febrer", "març", "abril", "maig", "juny",
             "juliol", "agost", "setembre", "octubre", "novembre", "desembre"]

MONTH_PATTERN = r'(?i)\b(gener|febrer|març|abril|maig|juny|juliol|agost|setembre|octubre|novembre|desembre)\b'


def _month_from_text(text_val):
    if not text_val:
        return None
    import re
    m = re.search(MONTH_PATTERN, str(text_val))
    return m.group(1).lower() if m else None


def _month_from_date(date_val):
    if not date_val:
        return None
    try:
        if hasattr(date_val, "month"):
            return MONTHS_CA[date_val.month - 1]
        dt = datetime.strptime(str(date_val)[:10], "%Y-%m-%d")
        return MONTHS_CA[dt.month - 1]
    except Exception:
        return None


def _load_mysql_maps(engine):
    with engine.connect() as conn:
        action_map = {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text(
                "SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"
            ))
        }
        project_lookup = {
            (r[0], str(r[1]).strip()): r[2]
            for r in conn.execute(text(
                "SELECT action_id, code, id FROM projects WHERE action_id IS NOT NULL AND code IS NOT NULL"
            ))
        }
        existing = {
            (r[0], str(r[1])[:10], str(r[2]))
            for r in conn.execute(text(
                "SELECT project_id, certification_date, net_amount FROM certifications"
            ))
        }
    return action_map, project_lookup, existing


def _insert_certifications(engine, records, existing):
    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        for r in records:
            key = (r["project_id"], str(r["certification_date"])[:10], str(r["net_amount"]))
            if key in existing:
                skipped += 1
                continue
            existing.add(key)
            conn.execute(text("""
                INSERT INTO certifications
                    (id, project_id, month, amount, net_amount, certification_date, created_at, updated_at)
                VALUES
                    (:id, :project_id, :month, :amount, :net_amount, :certification_date, :created_at, :updated_at)
            """), {**r, "created_at": now, "updated_at": now})
            inserted += 1
        conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# AT — Oracle
# ---------------------------------------------------------------------------

def _extract_at_oracle(oracle_config, action_map, project_lookup, tasca_to_at):
    records = []

    AMOUNT_COLS = ["IMPORTFACTURAR", "IMPORTFACTURAT", "IMPORTFACTURA"]
    DATE_COLS = ["DATAFACTURA", "DATAFACTURACIO"]

    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT PROJECTE, CASEID FROM PIO_ACTUACIONS WHERE PROJECTE IS NOT NULL AND CASEID IS NOT NULL")
        proj_to_case = {str(int(float(r[0]))): str(int(float(r[1]))) for r in cursor.fetchall()}

        cursor.execute("SELECT IDTASQUES, NOMTASCA FROM PIO_TASQUES")
        tasques = {str(int(float(r[0]))): (r[1] or "").strip() for r in cursor.fetchall()}

        cursor.execute("SELECT IDPROJECTE, " + ", ".join(f"TASCAFACTURACIO{i}" for i in range(1, 21)) + " FROM PIO_PROJECTE")
        proj_slot_map = {}
        for row in cursor.fetchall():
            idproj = str(int(float(row[0]))) if row[0] else None
            if not idproj:
                continue
            for i, val in enumerate(row[1:], start=1):
                if val:
                    tasca_id = str(int(float(val)))
                    nomtasca = tasques.get(tasca_id, "")
                    at_code = tasca_to_at.get(nomtasca)
                    proj_slot_map[(idproj, i)] = (nomtasca, at_code)

        for i in range(1, 21):
            table = f"PIO_FACTURACIO{i}" if i != 3 else "FACTURACIO3"

            # Discover column names
            try:
                cursor.execute(f"SELECT * FROM {table} WHERE ROWNUM = 1")
                cols = [d[0] for d in cursor.description]
            except Exception:
                continue

            amount_col = next((c for c in AMOUNT_COLS if c in cols), None)
            date_col = next((c for c in DATE_COLS if c in cols), None)
            if not amount_col or not date_col:
                print(f"  {table}: cannot find amount/date columns, skipping")
                continue

            try:
                cursor.execute(f"SELECT PROJECTE, {amount_col}, {date_col}, CONCEPTE FROM {table}")
                rows = cursor.fetchall()
            except Exception as e:
                print(f"  {table}: query failed ({e}), skipping")
                continue

            for row in rows:
                proj_ora = str(int(float(row[0]))) if row[0] else None
                if not proj_ora:
                    continue
                caseid = proj_to_case.get(proj_ora)
                if not caseid:
                    continue
                action_id = action_map.get(caseid)
                if not action_id:
                    continue

                slot_info = proj_slot_map.get((proj_ora, i))
                if not slot_info or not slot_info[1]:
                    continue
                at_code = slot_info[1]
                project_id = project_lookup.get((action_id, at_code))
                if not project_id:
                    continue

                import_base = float(row[1]) if row[1] is not None else None
                if import_base is None:
                    continue

                cert_date = row[2]
                concepte = row[3]

                month = _month_from_text(concepte) or _month_from_date(cert_date)
                if not month:
                    month = _month_from_date(cert_date)
                if not month:
                    month = "desconegut"

                records.append({
                    "id": str(uuid.uuid4()),
                    "project_id": project_id,
                    "month": month,
                    "net_amount": round(import_base, 2),
                    "amount": round(import_base * 1.21, 2),
                    "certification_date": cert_date,
                })

        cursor.close()
    return records


# ---------------------------------------------------------------------------
# AT — CSV fallback
# ---------------------------------------------------------------------------

def _extract_at_csv(action_map, project_lookup, tasca_to_at):
    records = []

    # proj_to_case
    proj_to_case = {}
    with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            proj = str(row.get("PROJECTE", "")).split(".")[0].strip()
            caseid = str(row.get("CASEID", "")).strip()
            if proj and caseid:
                proj_to_case[proj] = caseid

    # tasques
    tasques = {}
    with open("data/oracle_export/PIO_TASQUES.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            tasques[str(row.get("IDTASQUES", "")).strip()] = row.get("NOMTASCA", "").strip()

    # proj_slot_map
    proj_slot_map = {}
    with open("data/oracle_export/PIO_PROJECTE.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            idproj = str(row.get("IDPROJECTE", "")).strip()
            if not idproj:
                continue
            for i in range(1, 21):
                val = str(row.get(f"TASCAFACTURACIO{i}", "")).split(".")[0].strip()
                if val:
                    nomtasca = tasques.get(val, "")
                    at_code = tasca_to_at.get(nomtasca)
                    proj_slot_map[(idproj, i)] = (nomtasca, at_code)

    AMOUNT_COLS = ["IMPORTFACTURAR", "IMPORTFACTURAT", "IMPORTFACTURA"]
    DATE_COLS = ["DATAFACTURA", "DATAFACTURACIO"]

    for i in range(1, 21):
        fname = f"PIO_FACTURACIO{i}" if i != 3 else "FACTURACIO3"
        path = f"data/oracle_export/{fname}.csv"
        try:
            with open(path, encoding="utf-8-sig", errors="replace") as f:
                reader = csv.DictReader(f)
                cols = reader.fieldnames or []
                amount_col = next((c for c in AMOUNT_COLS if c in cols), None)
                date_col = next((c for c in DATE_COLS if c in cols), None)
                if not amount_col or not date_col:
                    print(f"  {fname}.csv: cannot find amount/date columns, skipping")
                    continue
                for row in reader:
                    proj_ora = str(row.get("PROJECTE", "")).split(".")[0].strip()
                    if not proj_ora:
                        continue
                    caseid = proj_to_case.get(proj_ora)
                    if not caseid:
                        continue
                    action_id = action_map.get(caseid)
                    if not action_id:
                        continue
                    slot_info = proj_slot_map.get((proj_ora, i))
                    if not slot_info or not slot_info[1]:
                        continue
                    at_code = slot_info[1]
                    project_id = project_lookup.get((action_id, at_code))
                    if not project_id:
                        continue

                    raw_amount = str(row.get(amount_col, "")).strip()
                    try:
                        import_base = float(raw_amount)
                    except ValueError:
                        continue

                    cert_date_str = str(row.get(date_col, "")).strip()
                    try:
                        cert_date = datetime.strptime(cert_date_str[:10], "%Y-%m-%d").date()
                    except Exception:
                        continue

                    concepte = row.get("CONCEPTE", "")
                    month = _month_from_text(concepte) or _month_from_date(cert_date) or "desconegut"

                    records.append({
                        "id": str(uuid.uuid4()),
                        "project_id": project_id,
                        "month": month,
                        "net_amount": round(import_base, 2),
                        "amount": round(import_base * 1.21, 2),
                        "certification_date": cert_date,
                    })
        except FileNotFoundError:
            print(f"  {fname}.csv not found, skipping slot {i}")

    return records


# ---------------------------------------------------------------------------
# Obra — Oracle
# ---------------------------------------------------------------------------

def _extract_obra_oracle(oracle_config, action_map, project_lookup):
    records = []
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT OBRA, CASEID FROM PIO_ACTUACIONS WHERE OBRA IS NOT NULL AND CASEID IS NOT NULL")
        obra_to_case = {}
        for obra, caseid in cursor.fetchall():
            obra_val = str(int(float(obra)))
            if obra_val and obra_val != "0":
                obra_to_case[obra_val] = str(int(float(caseid)))

        cursor.execute("SELECT OBRA, MESCERTIFICACIO, IMPORTAMBIVA, DATACERTIFICACIO FROM PIO_CERTIFICACIONS")
        for row in cursor.fetchall():
            obra_val = str(int(float(row[0]))) if row[0] else None
            if not obra_val:
                continue
            caseid = obra_to_case.get(obra_val)
            if not caseid:
                continue
            action_id = action_map.get(caseid)
            if not action_id:
                continue
            project_id = project_lookup.get((action_id, "Obra"))
            if not project_id:
                continue

            import_amb_iva = float(row[1]) if row[1] is not None else None
            # MESCERTIFICACIO is index 1 but field order is OBRA, MESCERTIFICACIO, IMPORTAMBIVA, DATACERTIFICACIO
            mes = row[1]
            import_amb_iva = float(row[2]) if row[2] is not None else None
            cert_date = row[3]

            if import_amb_iva is None:
                continue

            month = _month_from_text(mes) or _month_from_date(cert_date) or "desconegut"

            records.append({
                "id": str(uuid.uuid4()),
                "project_id": project_id,
                "month": month,
                "net_amount": round(import_amb_iva / 1.21, 2),
                "amount": round(import_amb_iva, 2),
                "certification_date": cert_date,
            })
        cursor.close()
    return records


# ---------------------------------------------------------------------------
# Obra — CSV fallback
# ---------------------------------------------------------------------------

def _extract_obra_csv(action_map, project_lookup):
    records = []

    obra_to_case = {}
    with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            caseid = str(row.get("CASEID", "")).strip()
            obra = str(row.get("OBRA", "")).split(".")[0].strip()
            if obra and obra != "0" and caseid:
                obra_to_case[obra] = caseid

    with open("data/oracle_export/PIO_CERTIFICACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            obra_val = str(row.get("OBRA", "")).split(".")[0].strip()
            if not obra_val:
                continue
            caseid = obra_to_case.get(obra_val)
            if not caseid:
                continue
            action_id = action_map.get(caseid)
            if not action_id:
                continue
            project_id = project_lookup.get((action_id, "Obra"))
            if not project_id:
                continue

            raw = str(row.get("IMPORTAMBIVA", "")).strip()
            try:
                import_amb_iva = float(raw)
            except ValueError:
                continue

            cert_date_str = str(row.get("DATACERTIFICACIO", "")).strip()
            try:
                cert_date = datetime.strptime(cert_date_str[:10], "%Y-%m-%d").date()
            except Exception:
                continue

            mes = row.get("MESCERTIFICACIO", "")
            month = _month_from_text(mes) or _month_from_date(cert_date) or "desconegut"

            records.append({
                "id": str(uuid.uuid4()),
                "project_id": project_id,
                "month": month,
                "net_amount": round(import_amb_iva / 1.21, 2),
                "amount": round(import_amb_iva, 2),
                "certification_date": cert_date,
            })
    return records


# ---------------------------------------------------------------------------
# Funcions públiques
# ---------------------------------------------------------------------------

def migrate_certifications_AT(mysql_url, oracle_config=None, json_file="data/map_assistance_type.json"):
    print("Starting AT Certifications migration...")

    with open(json_file, encoding="utf-8") as f:
        raw = json.load(f)
    tasca_to_at = {}
    for at_code, names in raw.items():
        for name in names:
            tasca_to_at[name.strip()] = at_code

    engine = create_engine(mysql_url)
    action_map, project_lookup, existing = _load_mysql_maps(engine)

    print("  Extracting from Oracle...")
    if oracle_config:
        try:
            records = _extract_at_oracle(oracle_config, action_map, project_lookup, tasca_to_at)
            print(f"  Oracle source: {len(records)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            records = _extract_at_csv(action_map, project_lookup, tasca_to_at)
            print(f"  CSV fallback: {len(records)} rows")
    else:
        records = _extract_at_csv(action_map, project_lookup, tasca_to_at)
        print(f"  CSV fallback: {len(records)} rows")

    inserted, skipped = _insert_certifications(engine, records, existing)
    print(f"  AT Certifications: {inserted} inserted, {skipped} already existed.")


def migrate_certifications_obra(mysql_url, oracle_config=None):
    print("Starting Obra Certifications migration...")

    engine = create_engine(mysql_url)
    action_map, project_lookup, existing = _load_mysql_maps(engine)

    print("  Extracting from Oracle...")
    if oracle_config:
        try:
            records = _extract_obra_oracle(oracle_config, action_map, project_lookup)
            print(f"  Oracle source: {len(records)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            records = _extract_obra_csv(action_map, project_lookup)
            print(f"  CSV fallback: {len(records)} rows")
    else:
        records = _extract_obra_csv(action_map, project_lookup)
        print(f"  CSV fallback: {len(records)} rows")

    inserted, skipped = _insert_certifications(engine, records, existing)
    print(f"  Obra Certifications: {inserted} inserted, {skipped} already existed.")
