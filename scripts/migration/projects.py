import csv
import json
import uuid
import oracledb
from datetime import datetime
from sqlalchemy import create_engine, text
from helpers.pg_connection import get_pg_connection


# Migrar projects:
# Dos tipus: AT (Assistència Tècnica) i Obra
# Cada AT és un contracte per una tipologia concreta dins d'una acció
# Cada Obra és un únic contracte d'obra per acció
#
# Fonts:
#   AT  -> PostgreSQL dadeseconomiques_at  (ATs ja executats, tenen import)
#        + Oracle PIO_PROJECTE             (ATs registrats al gestor, poden solapar-se)
#   Obra -> PostgreSQL dadeseconomiques_obra (una per acció)
#
# Idempotència: per (action_id, code) quan hi ha codi AT; sempre insereix si no hi ha codi (i avisa)


def _load_mysql_maps(engine):
    with engine.connect() as conn:
        action_map = {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text("SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"))
        }
        pt_row = conn.execute(text("SELECT id FROM project_types WHERE `key` = 'external' LIMIT 1")).fetchone()
        if not pt_row:
            raise Exception("Project type 'external' not found in project_types")
        external_type_id = pt_row[0]

        existing = {
            (r[0], r[1])
            for r in conn.execute(text("SELECT action_id, code FROM projects WHERE action_id IS NOT NULL AND code IS NOT NULL"))
        }
    return action_map, external_type_id, existing


def _insert_projects(engine, records):
    now = datetime.now()
    inserted = skipped = no_code = 0
    with engine.connect() as conn:
        # Recargar existing por si ha cambiado
        existing = {
            (r[0], r[1])
            for r in conn.execute(text("SELECT action_id, code FROM projects WHERE action_id IS NOT NULL AND code IS NOT NULL"))
        }
        for r in records:
            action_id = r["action_id"]
            code = r.get("code")

            if code:
                if (action_id, code) in existing:
                    skipped += 1
                    continue
                existing.add((action_id, code))
            else:
                no_code += 1

            conn.execute(text("""
                INSERT INTO projects
                    (id, name, project_type_id, code, description, accounting_code,
                     action_id, phase, created_at, updated_at)
                VALUES
                    (:id, :name, :project_type_id, :code, :description, :accounting_code,
                     :action_id, :phase, :created_at, :updated_at)
            """), {**r, "created_at": now, "updated_at": now})
            inserted += 1

        conn.commit()
    return inserted, skipped, no_code


# ---------------------------------------------------------------------------
# AT desde PostgreSQL
# ---------------------------------------------------------------------------

def _extract_at_pg(valid_cases):
    rows = []
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql("SELECT cas, tipologia_assistencia, codi_tipologia FROM dadeseconomiques_at", conn)
        conn.close()
        for _, row in df.iterrows():
            cas = str(row["cas"]).split(".")[0].strip()
            if cas not in valid_cases:
                continue
            rows.append({
                "cas": cas,
                "name": str(row["tipologia_assistencia"] or "AT sense nom").strip()[:191],
                "code": str(row["codi_tipologia"] or "").strip() or None,
            })
    except Exception as e:
        print(f"  PG error: {e}")
    return rows


def _extract_at_pg_csv(valid_cases):
    rows = []
    with open("data/pg_export/dadeseconomiques_at.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            cas = str(row.get("cas", "")).split(".")[0].strip()
            if cas not in valid_cases:
                continue
            rows.append({
                "cas": cas,
                "name": str(row.get("tipologia_assistencia") or "AT sense nom").strip()[:191],
                "code": str(row.get("codi_tipologia") or "").strip() or None,
            })
    return rows


# ---------------------------------------------------------------------------
# AT desde Oracle
# ---------------------------------------------------------------------------

def _load_tasca_to_at(json_file):
    with open(json_file, encoding="utf-8") as f:
        raw = json.load(f)
    mapping = {}
    for at_code, names in raw.items():
        for name in names:
            mapping[name.strip()] = at_code
    return mapping


def _extract_at_oracle(oracle_config, valid_cases, tasca_to_at):
    rows = []
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()

        # IDPROJECTE -> CASEID
        cursor.execute("SELECT PROJECTE, CASEID FROM PIO_ACTUACIONS WHERE PROJECTE IS NOT NULL AND CASEID IS NOT NULL")
        proj_to_case = {str(int(float(r[0]))): str(int(float(r[1]))) for r in cursor.fetchall()}

        # IDTASQUES -> NOMTASCA
        cursor.execute("SELECT IDTASQUES, NOMTASCA FROM PIO_TASQUES")
        tasques = {str(int(float(r[0]))): (r[1] or "").strip() for r in cursor.fetchall()}

        # Recorrer PIO_PROJECTE slot por slot
        cursor.execute(f"SELECT IDPROJECTE, {', '.join(f'TASCAFACTURACIO{i}' for i in range(1,21))} FROM PIO_PROJECTE")
        for row in cursor.fetchall():
            idproj = str(int(float(row[0]))) if row[0] else None
            caseid = proj_to_case.get(idproj)
            if not caseid or caseid not in valid_cases:
                continue
            for i, tasca_val in enumerate(row[1:], start=1):
                if not tasca_val:
                    continue
                tasca_id = str(int(float(tasca_val)))
                nomtasca = tasques.get(tasca_id, "")
                at_code = tasca_to_at.get(nomtasca)
                rows.append({
                    "cas": caseid,
                    "name": nomtasca or f"AT slot {i}",
                    "code": at_code,
                })
        cursor.close()
    return rows


def _extract_at_oracle_csv(valid_cases, tasca_to_at):
    # IDPROJECTE -> CASEID
    proj_to_case = {}
    with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            caseid = str(row.get("CASEID", "")).strip()
            proj = str(row.get("PROJECTE", "")).split(".")[0].strip()
            if caseid in valid_cases and proj:
                proj_to_case[proj] = caseid

    # IDTASQUES -> NOMTASCA
    tasques = {}
    with open("data/oracle_export/PIO_TASQUES.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            tasques[str(row.get("IDTASQUES", "")).strip()] = row.get("NOMTASCA", "").strip()

    rows = []
    with open("data/oracle_export/PIO_PROJECTE.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            idproj = str(row.get("IDPROJECTE", "")).strip()
            caseid = proj_to_case.get(idproj)
            if not caseid:
                continue
            for i in range(1, 21):
                tasca_id = str(row.get(f"TASCAFACTURACIO{i}", "")).split(".")[0].strip()
                if not tasca_id:
                    continue
                nomtasca = tasques.get(tasca_id, "")
                at_code = tasca_to_at.get(nomtasca)
                rows.append({
                    "cas": caseid,
                    "name": nomtasca or f"AT slot {i}",
                    "code": at_code,
                })
    return rows


# ---------------------------------------------------------------------------
# Obra desde PostgreSQL
# ---------------------------------------------------------------------------

def _extract_obra_pg(valid_cases):
    rows = []
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql("SELECT cas FROM dadeseconomiques_obra", conn)
        conn.close()
        for _, row in df.iterrows():
            cas = str(row["cas"]).split(".")[0].strip()
            if cas in valid_cases:
                rows.append(cas)
    except Exception as e:
        print(f"  PG error: {e}")
    return rows


def _extract_obra_pg_csv(valid_cases):
    rows = []
    with open("data/pg_export/dadeseconomiques_obra.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            cas = str(row.get("cas", "")).split(".")[0].strip()
            if cas in valid_cases:
                rows.append(cas)
    return rows


# ---------------------------------------------------------------------------
# Obra desde Oracle
# ---------------------------------------------------------------------------

def _extract_obra_oracle(oracle_config, valid_cases):
    rows = []
    with oracledb.connect(**oracle_config) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT CASEID, OBRA FROM PIO_ACTUACIONS WHERE OBRA IS NOT NULL AND CASEID IS NOT NULL")
        for caseid, obra in cursor.fetchall():
            cas = str(int(float(caseid)))
            obra_val = str(int(float(obra))) if obra else ""
            if cas in valid_cases and obra_val and obra_val != "0":
                rows.append(cas)
        cursor.close()
    return rows


def _extract_obra_oracle_csv(valid_cases):
    rows = []
    with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            caseid = str(row.get("CASEID", "")).strip()
            obra = str(row.get("OBRA", "")).split(".")[0].strip()
            if caseid in valid_cases and obra and obra != "0":
                rows.append(caseid)
    return rows


# ---------------------------------------------------------------------------
# Funciones públicas
# ---------------------------------------------------------------------------

def migrate_projects_AT(mysql_url, oracle_config=None, valid_cases=None):
    print("Starting AT Projects migration...")
    if valid_cases is None:
        with open("data/casos_planificacions.json") as f:
            valid_cases = set(json.load(f))

    engine = create_engine(mysql_url)
    action_map, external_type_id, _ = _load_mysql_maps(engine)
    tasca_to_at = _load_tasca_to_at("data/map_assistance_type.json")

    # --- Extraer ATs de PostgreSQL ---
    print("  Extracting from PostgreSQL...")
    pg_rows = _extract_at_pg(valid_cases)
    if pg_rows:
        print(f"  PG source: {len(pg_rows)} rows")
    else:
        pg_rows = _extract_at_pg_csv(valid_cases)
        print(f"  PG CSV fallback: {len(pg_rows)} rows")

    # --- Extraer ATs de Oracle ---
    print("  Extracting from Oracle...")
    if oracle_config:
        try:
            ora_rows = _extract_at_oracle(oracle_config, valid_cases, tasca_to_at)
            print(f"  Oracle source: {len(ora_rows)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            ora_rows = _extract_at_oracle_csv(valid_cases, tasca_to_at)
            print(f"  Oracle CSV fallback: {len(ora_rows)} rows")
    else:
        ora_rows = _extract_at_oracle_csv(valid_cases, tasca_to_at)
        print(f"  Oracle CSV fallback: {len(ora_rows)} rows")

    # --- Construir registros ---
    records = []
    for r in pg_rows + ora_rows:
        action_id = action_map.get(r["cas"])
        if not action_id:
            continue
        code = r["code"]
        if not code:
            print(f"  WARNING: AT sin código para cas={r['cas']} name='{r['name']}' — se insertará igualmente")
        records.append({
            "id": str(uuid.uuid4()),
            "name": r["name"],
            "description": r["name"],
            "code": code,
            "accounting_code": code,
            "project_type_id": external_type_id,
            "action_id": action_id,
            "phase": 0,
        })

    inserted, skipped, no_code = _insert_projects(engine, records)
    print(f"  AT Projects: {inserted} inserted, {skipped} already existed, {no_code} without code (warnings above).")


def migrate_projects_obra(mysql_url, oracle_config=None, valid_cases=None):
    print("Starting Obra Projects migration...")
    if valid_cases is None:
        with open("data/casos_planificacions.json") as f:
            valid_cases = set(json.load(f))

    engine = create_engine(mysql_url)
    action_map, external_type_id, existing = _load_mysql_maps(engine)

    # --- Extraer Obras de PG ---
    print("  Extracting from PostgreSQL...")
    pg_cases = _extract_obra_pg(valid_cases)
    if pg_cases:
        print(f"  PG source: {len(pg_cases)} rows")
    else:
        pg_cases = _extract_obra_pg_csv(valid_cases)
        print(f"  PG CSV fallback: {len(pg_cases)} rows")

    # --- Extraer Obras de Oracle ---
    print("  Extracting from Oracle...")
    if oracle_config:
        try:
            ora_cases = _extract_obra_oracle(oracle_config, valid_cases)
            print(f"  Oracle source: {len(ora_cases)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")
            ora_cases = _extract_obra_oracle_csv(valid_cases)
            print(f"  Oracle CSV fallback: {len(ora_cases)} rows")
    else:
        ora_cases = _extract_obra_oracle_csv(valid_cases)
        print(f"  Oracle CSV fallback: {len(ora_cases)} rows")

    cases = list(set(pg_cases + ora_cases))

    # Deduplicar por cas
    seen = set()
    records = []
    for cas in cases:
        if cas in seen:
            continue
        seen.add(cas)
        action_id = action_map.get(cas)
        if not action_id:
            continue
        records.append({
            "id": str(uuid.uuid4()),
            "name": "Obra",
            "description": "Obra",
            "code": "Obra",
            "accounting_code": "Obra",
            "project_type_id": external_type_id,
            "action_id": action_id,
            "phase": 0,
        })

    inserted, skipped, _ = _insert_projects(engine, records)
    print(f"  Obra Projects: {inserted} inserted, {skipped} already existed.")
