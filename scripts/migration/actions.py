import csv
import json
import uuid
import unicodedata
import oracledb
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text


# Campos migrados:
# tracking_code = CASEID
# name         = TITOL
# alias        = ALIES
# description  = DESCRIPCIOPROJECTE (de PIO_PROJECTE)
# service_id   = SERVEIPO -> PIO_SERVEI_PO.CODI -> services.key
# action_type_id / action_subtype_id -> via update_types()
# address      = ADRECA

# Campos sin datos suficientes (se dejan NULL):
# owner_id, assistance_type_id, priority_id, request_channel_id
# status, started_at, ended_at, holded_at, phase_*_finished_at
# activity_type_id, subsidy_*, accounting_project_code
# area, comments, objectives_description, target_date_*


def _normalize_city(name):
    """Normaliza nombre de municipio al formato key de la tabla cities."""
    s = str(name).lower().strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", errors="ignore").decode("utf-8")
    s = s.replace("'", "")
    s = " ".join(s.split()).replace(" ", "-")
    if s == "hospitalet-de-llobregat":
        s = "lhospitalet-de-llobregat"
    return s


# ---------------------------------------------------------------------------
# Extracción
# ---------------------------------------------------------------------------

def _extract_actions_oracle(oracle_config, valid_cases):
    with oracledb.connect(**oracle_config) as conn:
        query = """
            SELECT
                a.CASEID, a.TITOL, a.ALIES, a.ADRECA,
                a.ETIQUETAMUNICIPIS,
                p.DESCRIPCIOPROJECTE,
                COALESCE(sObra.CODI, sPo.CODI) AS SERVICE_KEY,
                t.TIPUSACTUACIO AS OLD_TYPE,
                sub.SUBTIPUSACTUACIO AS OLD_SUBTYPE
            FROM PIO_ACTUACIONS a
            LEFT JOIN PIO_PROJECTE p ON a.PROJECTE = p.IDPROJECTE
            LEFT JOIN PIO_SERVEI_PO sObra ON a.SERVEIPOOBRA = sObra.IDSERVEI_PO
            LEFT JOIN PIO_SERVEI_PO sPo   ON a.SERVEIPO     = sPo.IDSERVEI_PO
            LEFT JOIN PIO_SUBTIPUSDACTUACIO sub ON p.SUBTIPUSACTUACIO = sub.IDSUBTIPUSDACTUACIO
            LEFT JOIN PIO_TIPUSDACTUACIO t ON sub.IDTIPUSDACTUACIO = t.IDTIPUSDACTUACIO
        """
        df = pd.read_sql(query, conn)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: x.read() if hasattr(x, "read") else x)
    df["CASEID_STR"] = df["CASEID"].astype(str).str.strip()
    return df[df["CASEID_STR"].isin(valid_cases)]


def _extract_actions_csv(valid_cases):
    # PIO_ACTUACIONS
    act = {}
    with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            caseid = str(row.get("CASEID", "")).strip()
            if caseid in valid_cases:
                act[caseid] = row

    # PIO_PROJECTE indexado por IDPROJECTE
    proj = {}
    with open("data/oracle_export/PIO_PROJECTE.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            proj[str(row.get("IDPROJECTE", "")).strip()] = row

    # PIO_SERVEI_PO indexado por IDSERVEI_PO
    serv = {}
    with open("data/oracle_export/PIO_SERVEI_PO.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            serv[str(row.get("IDSERVEI_PO", "")).strip()] = row

    # PIO_SUBTIPUSDACTUACIO
    subtipus = {}
    with open("data/oracle_export/PIO_SUBTIPUSDACTUACIO.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            subtipus[str(row.get("IDSUBTIPUSDACTUACIO", "")).strip()] = row

    # PIO_TIPUSDACTUACIO
    tipusd = {}
    with open("data/oracle_export/PIO_TIPUSDACTUACIO.csv", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            tipusd[str(row.get("IDTIPUSDACTUACIO", "")).strip()] = row

    rows = []
    for caseid, a in act.items():
        p = proj.get(str(a.get("PROJECTE", "")).split(".")[0].strip(), {})
        sub_id = str(p.get("SUBTIPUSACTUACIO", "")).split(".")[0].strip()
        sub = subtipus.get(sub_id, {})
        tip = tipusd.get(str(sub.get("IDTIPUSDACTUACIO", "")).strip(), {})

        # SERVEIPOOBRA es prioritario, SERVEIPO como fallback
        obra_id = str(a.get("SERVEIPOOBRA", "")).split(".")[0].strip()
        po_id   = str(a.get("SERVEIPO", "")).split(".")[0].strip()
        s = serv.get(obra_id or po_id, {})

        rows.append({
            "CASEID": caseid,
            "CASEID_STR": caseid,
            "TITOL": a.get("TITOL", ""),
            "ALIES": a.get("ALIES", ""),
            "ADRECA": a.get("ADRECA", ""),
            "ETIQUETAMUNICIPIS": a.get("ETIQUETAMUNICIPIS", ""),
            "DESCRIPCIOPROJECTE": p.get("DESCRIPCIOPROJECTE", ""),
            "SERVICE_KEY": s.get("CODI", ""),
            "OLD_TYPE": tip.get("TIPUSACTUACIO", ""),
            "OLD_SUBTYPE": sub.get("SUBTIPUSACTUACIO", ""),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# migrate_actions
# ---------------------------------------------------------------------------

def migrate_actions(mysql_url, oracle_config, valid_cases):
    print("Starting Actions migration...")

    engine = create_engine(mysql_url)

    # Cargar mapas de MySQL
    with engine.connect() as conn:
        services_df = pd.read_sql("SELECT id, `key` FROM services", conn)
        service_map = dict(zip(services_df["key"].str.strip(), services_df["id"]))

        at_df = pd.read_sql("SELECT id, `key` FROM action_types", conn)
        type_map = dict(zip(at_df["key"], at_df["id"]))

        existing_codes = {
            str(r[0]) for r in conn.execute(text("SELECT tracking_code FROM actions WHERE tracking_code IS NOT NULL"))
        }

    # Cargar mapping JSON de tipos
    with open("data/map_type_subtype.json", encoding="utf-8") as f:
        mapping_json = json.load(f)

    old_to_new_type = {}
    old_to_new_subtype = {}
    for type_key, content in mapping_json.items():
        for old_name in content.get("tipus", []):
            old_to_new_type[old_name.strip()] = type_key
        for subtype_key, old_list in content.get("subtipus", {}).items():
            for old_name in old_list:
                old_to_new_subtype[old_name.strip()] = subtype_key

    # Extracción
    df = pd.DataFrame()
    if oracle_config:
        try:
            df = _extract_actions_oracle(oracle_config, valid_cases)
            print(f"  Source: Oracle — {len(df)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")

    if df.empty:
        df = _extract_actions_csv(valid_cases)
        print(f"  Source: CSV — {len(df)} rows")

    if df.empty:
        print("No actions found.")
        return

    # Transformación
    now = datetime.now()
    to_insert = []
    to_update = []

    for _, row in df.iterrows():
        code = str(row["CASEID_STR"]).strip()

        svc_key = str(row.get("SERVICE_KEY") or "").strip()
        service_id = service_map.get(svc_key)

        old_type = str(row.get("OLD_TYPE") or "").strip()
        old_subtype = str(row.get("OLD_SUBTYPE") or "").strip()
        new_type_key = old_to_new_type.get(old_type)
        new_subtype_key = old_to_new_subtype.get(old_subtype)
        action_type_id = type_map.get(new_type_key) if new_type_key else None
        action_subtype_id = type_map.get(new_subtype_key) if new_subtype_key else None

        desc = str(row.get("DESCRIPCIOPROJECTE") or "").strip() or None
        if desc and len(desc) > 500:
            desc = desc[:500]

        record = {
            "tracking_code": int(float(code)),
            "name": str(row.get("TITOL") or "").strip(),
            "alias": str(row.get("ALIES") or "").strip() or None,
            "description": desc,
            "address": str(row.get("ADRECA") or "").strip() or None,
            "service_id": service_id,
            "action_type_id": action_type_id,
            "action_subtype_id": action_subtype_id,
            "updated_at": now,
        }

        if code in existing_codes:
            to_update.append(record)
        else:
            to_insert.append({**record, "id": str(uuid.uuid4()), "created_at": now})

    # Carga
    inserted = updated = 0
    with engine.connect() as conn:
        for r in to_insert:
            conn.execute(text("""
                INSERT INTO actions
                    (id, tracking_code, name, alias, description, address,
                     service_id, action_type_id, action_subtype_id, created_at, updated_at)
                VALUES
                    (:id, :tracking_code, :name, :alias, :description, :address,
                     :service_id, :action_type_id, :action_subtype_id, :created_at, :updated_at)
            """), r)
            inserted += 1

        for r in to_update:
            conn.execute(text("""
                UPDATE actions SET
                    name = :name, alias = :alias, description = :description,
                    address = :address, service_id = :service_id,
                    action_type_id = :action_type_id, action_subtype_id = :action_subtype_id,
                    updated_at = :updated_at
                WHERE tracking_code = :tracking_code
            """), r)
            updated += 1

        conn.commit()

    without_type = sum(1 for r in to_insert + to_update if r["action_type_id"] is None)
    without_service = sum(1 for r in to_insert + to_update if r["service_id"] is None)
    print(f"Migration successful: {inserted} inserted, {updated} updated.")
    print(f"  Without type: {without_type} | Without service: {without_service}")


# ---------------------------------------------------------------------------
# migrate_action_city
# ---------------------------------------------------------------------------

def migrate_action_city(mysql_url, oracle_config=None):
    print("Starting Action <-> City migration...")

    engine = create_engine(mysql_url)

    with engine.connect() as conn:
        action_map = {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text("SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"))
        }
        city_map = {
            r[0].strip().lower(): r[1]
            for r in conn.execute(text("SELECT `key`, id FROM cities"))
        }
        existing_pairs = {
            (r[0], r[1])
            for r in conn.execute(text("SELECT action_id, city_id FROM action_city"))
        }

    # Extracción
    rows = []
    if oracle_config:
        try:
            with oracledb.connect(**oracle_config) as ora_conn:
                df = pd.read_sql(
                    "SELECT CASEID, ETIQUETAMUNICIPIS FROM PIO_ACTUACIONS WHERE ETIQUETAMUNICIPIS IS NOT NULL",
                    ora_conn
                )
            for _, row in df.iterrows():
                rows.append((str(row["CASEID"]).strip(), str(row["ETIQUETAMUNICIPIS"])))
            print(f"  Source: Oracle — {len(rows)} rows")
        except Exception as e:
            print(f"  Oracle failed ({e}), falling back to CSV")

    if not rows:
        with open("data/oracle_export/PIO_ACTUACIONS.csv", encoding="utf-8-sig", errors="replace") as f:
            for row in csv.DictReader(f):
                mun = row.get("ETIQUETAMUNICIPIS", "").strip()
                if mun:
                    rows.append((str(row.get("CASEID", "")).strip(), mun))
        print(f"  Source: CSV — {len(rows)} rows")

    # Transformación y carga
    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        for caseid, mun_raw in rows:
            action_id = action_map.get(caseid)
            if not action_id:
                continue

            for mun in mun_raw.split(","):
                city_key = _normalize_city(mun)
                city_id = city_map.get(city_key)
                if not city_id:
                    continue

                pair = (action_id, city_id)
                if pair in existing_pairs:
                    skipped += 1
                    continue

                conn.execute(text("""
                    INSERT INTO action_city (action_id, city_id, created_at, updated_at)
                    VALUES (:action_id, :city_id, :created_at, :updated_at)
                """), {"action_id": action_id, "city_id": city_id, "created_at": now, "updated_at": now})
                existing_pairs.add(pair)
                inserted += 1

        conn.commit()

    print(f"Migration successful: {inserted} inserted, {skipped} already existed.")
