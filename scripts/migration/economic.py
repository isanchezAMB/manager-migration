import csv
import uuid
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from helpers.pg_connection import get_pg_connection


# ---------------------------------------------------------------------------
# MySQL maps
# ---------------------------------------------------------------------------

def _load_action_map(engine):
    with engine.connect() as conn:
        return {
            str(int(float(r[0]))): r[1]
            for r in conn.execute(text(
                "SELECT tracking_code, id FROM actions WHERE tracking_code IS NOT NULL"
            ))
        }


def _load_existing_items(engine):
    with engine.connect() as conn:
        return {
            (r[0], str(r[1]).strip()): r[2]
            for r in conn.execute(text(
                "SELECT action_id, code, id FROM economic_items WHERE action_id IS NOT NULL AND code IS NOT NULL"
            ))
        }


def _load_existing_budgets(engine):
    with engine.connect() as conn:
        return {
            (r[0], int(r[1]))
            for r in conn.execute(text(
                "SELECT economic_item_id, year FROM economic_item_anual_budgets"
            ))
        }


# ---------------------------------------------------------------------------
# economic_items — AT
# ---------------------------------------------------------------------------

def _extract_at_items_pg(action_map):
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql(
            "SELECT cas, tipologia_assistencia, codi_tipologia, import_total FROM dadeseconomiques_at",
            conn
        )
        conn.close()
        return _build_at_records(df, action_map, col_cas="cas", col_name="tipologia_assistencia",
                                  col_code="codi_tipologia", col_amount="import_total")
    except Exception as e:
        print(f"  PG error: {e}")
        return []


def _extract_at_items_csv(action_map):
    import pandas as pd
    df = pd.read_csv("data/pg_export/dadeseconomiques_at.csv", encoding="utf-8-sig", dtype=str)
    return _build_at_records(df, action_map, col_cas="cas", col_name="tipologia_assistencia",
                              col_code="codi_tipologia", col_amount="import_total")


def _build_at_records(df, action_map, col_cas, col_name, col_code, col_amount):
    records = []
    for _, row in df.iterrows():
        cas = str(row[col_cas]).split(".")[0].strip()
        action_id = action_map.get(cas)
        if not action_id:
            continue
        code = str(row.get(col_code) or "").strip() or None
        name = str(row.get(col_name) or "Assistència Tècnica").strip()[:191]
        try:
            amount = float(row.get(col_amount) or 0)
        except (ValueError, TypeError):
            amount = 0.0
        records.append({
            "id": str(uuid.uuid4()),
            "action_id": action_id,
            "code": code,
            "name": name,
            "description": name,
            "type": "technical-assistance",
            "amount": round(amount, 2),
            "project_id": None,
        })
    return records


def _insert_items(engine, records, existing):
    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        for r in records:
            key = (r["action_id"], str(r["code"] or "").strip())
            if key in existing:
                skipped += 1
                continue
            existing[key] = r["id"]
            conn.execute(text("""
                INSERT INTO economic_items
                    (id, name, description, action_id, project_id, code, type, amount, created_at, updated_at)
                VALUES
                    (:id, :name, :description, :action_id, :project_id, :code, :type, :amount, :created_at, :updated_at)
            """), {**r, "created_at": now, "updated_at": now})
            inserted += 1
        conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# economic_items — Obra
# ---------------------------------------------------------------------------

def _extract_obra_items_pg(action_map):
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql("SELECT cas, pec_iva FROM dadeseconomiques_obra", conn)
        conn.close()
        return _build_obra_records(df, action_map, col_cas="cas", col_amount="pec_iva")
    except Exception as e:
        print(f"  PG error: {e}")
        return []


def _extract_obra_items_csv(action_map):
    import pandas as pd
    df = pd.read_csv("data/pg_export/dadeseconomiques_obra.csv", encoding="utf-8-sig", dtype=str)
    return _build_obra_records(df, action_map, col_cas="cas", col_amount="pec_iva")


def _build_obra_records(df, action_map, col_cas, col_amount):
    seen = set()
    records = []
    for _, row in df.iterrows():
        cas = str(row[col_cas]).split(".")[0].strip()
        action_id = action_map.get(cas)
        if not action_id or action_id in seen:
            continue
        seen.add(action_id)
        try:
            amount = float(row.get(col_amount) or 0)
        except (ValueError, TypeError):
            amount = 0.0
        records.append({
            "id": str(uuid.uuid4()),
            "action_id": action_id,
            "code": "Obra",
            "name": "Obra",
            "description": "Obra",
            "type": "work",
            "amount": round(amount, 2),
            "project_id": None,
        })
    return records


# ---------------------------------------------------------------------------
# economic_item_anual_budgets — AT
# ---------------------------------------------------------------------------

def _extract_at_budgets_pg(item_lookup):
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql(
            "SELECT id_at, anualitat, import_anualitat FROM dadeseconomiques_at_financament_anualitats",
            conn
        )
        conn.close()
        return _build_at_budget_records(df, item_lookup)
    except Exception as e:
        print(f"  PG error: {e}")
        return []


def _extract_at_budgets_csv(item_lookup):
    import pandas as pd
    df = pd.read_csv(
        "data/pg_export/dadeseconomiques_at_financament_anualitats.csv",
        encoding="utf-8-sig", dtype=str
    )
    return _build_at_budget_records(df, item_lookup)


def _build_at_budget_records(df, item_lookup):
    records = []
    for _, row in df.iterrows():
        id_at = str(row.get("id_at") or "").strip()
        # id_at format: XXXX_ATY  → tracking_code=XXXX, at_code=ATY
        if "_" not in id_at:
            continue
        parts = id_at.split("_", 1)
        tracking_code = parts[0].strip()
        at_code = parts[1].strip()

        item_id = item_lookup.get((tracking_code, at_code))
        if not item_id:
            continue

        try:
            year = int(float(str(row.get("anualitat") or 0)))
            amount = float(str(row.get("import_anualitat") or 0))
        except (ValueError, TypeError):
            continue

        records.append({
            "id": str(uuid.uuid4()),
            "economic_item_id": item_id,
            "year": year,
            "amount": round(amount, 2),
        })
    return records


# ---------------------------------------------------------------------------
# economic_item_anual_budgets — Obra
# ---------------------------------------------------------------------------

def _extract_obra_budgets_pg(obra_item_lookup):
    try:
        conn = get_pg_connection()
        import pandas as pd
        df = pd.read_sql(
            "SELECT cas, anualitat, import_anualitat FROM dadeseconomiques_obra_financament",
            conn
        )
        conn.close()
        return _build_obra_budget_records(df, obra_item_lookup)
    except Exception as e:
        print(f"  PG error: {e}")
        return []


def _extract_obra_budgets_csv(obra_item_lookup):
    import pandas as pd
    df = pd.read_csv(
        "data/pg_export/dadeseconomiques_obra_financament.csv",
        encoding="utf-8-sig", dtype=str
    )
    return _build_obra_budget_records(df, obra_item_lookup)


def _build_obra_budget_records(df, obra_item_lookup):
    records = []
    for _, row in df.iterrows():
        cas = str(row.get("cas") or "").split(".")[0].strip()
        item_id = obra_item_lookup.get(cas)
        if not item_id:
            continue
        try:
            year = int(float(str(row.get("anualitat") or 0)))
            amount = float(str(row.get("import_anualitat") or 0))
        except (ValueError, TypeError):
            continue
        records.append({
            "id": str(uuid.uuid4()),
            "economic_item_id": item_id,
            "year": year,
            "amount": round(amount, 2),
        })
    return records


# ---------------------------------------------------------------------------
# Insert budgets (shared)
# ---------------------------------------------------------------------------

def _insert_budgets(engine, records, existing):
    now = datetime.now()
    inserted = skipped = 0
    with engine.connect() as conn:
        for r in records:
            key = (r["economic_item_id"], r["year"])
            if key in existing:
                skipped += 1
                continue
            existing.add(key)
            conn.execute(text("""
                INSERT INTO economic_item_anual_budgets
                    (id, economic_item_id, amount, year, created_at, updated_at)
                VALUES
                    (:id, :economic_item_id, :amount, :year, :created_at, :updated_at)
            """), {**r, "created_at": now, "updated_at": now})
            inserted += 1
        conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_budgets(records, items_with_amounts, label):
    from collections import defaultdict
    sums = defaultdict(float)
    for r in records:
        sums[r["economic_item_id"]] += r["amount"]

    alerts = []
    for item_id, total in items_with_amounts.items():
        annual_sum = sums.get(item_id, 0.0)
        if not np.isclose(annual_sum, total, atol=0.05):
            alerts.append((item_id, annual_sum, total, annual_sum - total))

    if alerts:
        alerts.sort(key=lambda x: abs(x[3]), reverse=True)
        print(f"  [{label}] {len(alerts)} items where annual sum != total:")
        print(f"  {'ITEM_ID':<38} | {'ANNUAL SUM':>12} | {'TOTAL':>12} | {'DIFF':>10}")
        print(f"  {'-'*80}")
        for item_id, s, t, d in alerts[:20]:
            print(f"  {item_id:<38} | {s:>12.2f} | {t:>12.2f} | {d:>10.2f}")
        if len(alerts) > 20:
            print(f"  ... and {len(alerts) - 20} more")
    else:
        print(f"  [{label}] All annual sums match totals.")


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def migrate_economic_items_AT(mysql_url):
    print("Starting AT Economic Items migration...")
    engine = create_engine(mysql_url)
    action_map = _load_action_map(engine)
    existing = _load_existing_items(engine)

    print("  Extracting from PostgreSQL...")
    records = _extract_at_items_pg(action_map)
    if records:
        print(f"  PG source: {len(records)} rows")
    else:
        records = _extract_at_items_csv(action_map)
        print(f"  PG CSV fallback: {len(records)} rows")

    inserted, skipped = _insert_items(engine, records, existing)
    print(f"  AT Economic Items: {inserted} inserted, {skipped} already existed.")


def migrate_economic_items_obra(mysql_url):
    print("Starting Obra Economic Items migration...")
    engine = create_engine(mysql_url)
    action_map = _load_action_map(engine)
    existing = _load_existing_items(engine)

    print("  Extracting from PostgreSQL...")
    records = _extract_obra_items_pg(action_map)
    if records:
        print(f"  PG source: {len(records)} rows")
    else:
        records = _extract_obra_items_csv(action_map)
        print(f"  PG CSV fallback: {len(records)} rows")

    inserted, skipped = _insert_items(engine, records, existing)
    print(f"  Obra Economic Items: {inserted} inserted, {skipped} already existed.")


def migrate_economic_items_anual_budgets_AT(mysql_url):
    print("Starting AT Annual Budgets migration...")
    engine = create_engine(mysql_url)
    existing_budgets = _load_existing_budgets(engine)

    # Build item_lookup: (tracking_code, at_code) -> item_id
    # and items_with_amounts: item_id -> total_amount  (for validation)
    with engine.connect() as conn:
        rows = list(conn.execute(text("""
            SELECT ei.id, ei.code, ei.amount, a.tracking_code
            FROM economic_items ei
            JOIN actions a ON ei.action_id = a.id
            WHERE ei.type = 'technical-assistance' AND ei.code IS NOT NULL
        """)))

    item_lookup = {}
    items_with_amounts = {}
    for r in rows:
        tracking_code = str(int(float(r[3]))) if r[3] else None
        if tracking_code:
            item_lookup[(tracking_code, str(r[1]).strip())] = r[0]
            items_with_amounts[r[0]] = float(r[2] or 0)

    print("  Extracting from PostgreSQL...")
    records = _extract_at_budgets_pg(item_lookup)
    if records:
        print(f"  PG source: {len(records)} rows")
    else:
        records = _extract_at_budgets_csv(item_lookup)
        print(f"  PG CSV fallback: {len(records)} rows")

    _validate_budgets(records, items_with_amounts, "AT")

    inserted, skipped = _insert_budgets(engine, records, existing_budgets)
    print(f"  AT Annual Budgets: {inserted} inserted, {skipped} already existed.")


def migrate_economic_items_anual_budgets_obra(mysql_url, oracle_config=None):
    print("Starting Obra Annual Budgets migration...")
    engine = create_engine(mysql_url)
    existing_budgets = _load_existing_budgets(engine)

    # Build obra_item_lookup: tracking_code -> item_id
    with engine.connect() as conn:
        rows = list(conn.execute(text("""
            SELECT ei.id, ei.amount, a.tracking_code
            FROM economic_items ei
            JOIN actions a ON ei.action_id = a.id
            WHERE ei.type = 'work'
        """)))

    obra_item_lookup = {}
    items_with_amounts = {}
    for r in rows:
        tracking_code = str(int(float(r[2]))) if r[2] else None
        if tracking_code:
            obra_item_lookup[tracking_code] = r[0]
            items_with_amounts[r[0]] = float(r[1] or 0)

    print("  Extracting from PostgreSQL...")
    records = _extract_obra_budgets_pg(obra_item_lookup)
    if records:
        print(f"  PG source: {len(records)} rows")
    else:
        records = _extract_obra_budgets_csv(obra_item_lookup)
        print(f"  PG CSV fallback: {len(records)} rows")

    _validate_budgets(records, items_with_amounts, "Obra")

    inserted, skipped = _insert_budgets(engine, records, existing_budgets)
    print(f"  Obra Annual Budgets: {inserted} inserted, {skipped} already existed.")
