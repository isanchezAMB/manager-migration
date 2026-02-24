from sqlalchemy import create_engine
from helpers.pg_connection import get_pg_connection
import pandas as pd
from datetime import datetime
import uuid
import numpy as np

# migrar economic_items:
# AT
# prové de les dades de la nostra base de dades, de la taula dadeseconomiques_at
# name = tipologia_assistencia 
# code = codi_tipologia # !!!!!!hi ha uns quants que no tenen codi_tipologia perque son dos AT8 per un premi o coses semblants.
# desciption = tipologia_assistencia
# type = "technical-assistance"
# amount = import_total
# action_id = referència a la taula actions, la taula antiga té cas que fa referència al tracking_code de la taula actions, per tant haurem de fer un mapping amb el camp cas de dadeseconomiques_at com a tracking_code per trobar el uuid de l'acció
# project_id = DE MOMENT NO RELACIONEM, ELLS NO HO FAN (cercar mateix action_id i mateix codi_tipologia a la taula projects, per trobar el projecte que té aquesta assistència tècnica (a la taula projects, el camp code és el codi_tipologia i el camp action_id és el mateix que l'acció de l'economic_item))

def migrate_economic_items_AT(mysql_url):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting Economic Items (Technical Assistance) migration...")

    # Load dependencies
    try:
        # Retrieve tracking_code to link with Postgres 'cas'
        actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        action_map = dict(zip(actions_df['tracking_code'], actions_df['id']))

    except Exception as e:
        print(f"MySQL dependencies error: {e}")
        return

    # Read data from Postgres
    print("Reading from Postgres table dadeseconomiques_at...")
    df = pd.DataFrame()
    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        
        query = """
            SELECT 
                cas, 
                tipologia_assistencia, 
                codi_tipologia, 
                import_total 
            FROM dadeseconomiques_at
        """
        df = pd.read_sql(query, pg_conn)
        print(f"Read {len(df)} rows.")
        
    except Exception as e:
        print(f"Error reading Postgres: {e}")
        return
    finally:
        if pg_conn: pg_conn.close()

    # Data transformation
    if not df.empty:
        print("Transforming data...")
        
        # Clean tracking code and map action_id
        df['clean_code'] = pd.to_numeric(df['cas'], errors='coerce').fillna(0).astype(int).astype(str)
        df['action_id'] = df['clean_code'].map(action_map)
        
        # Filter out orphan records
        df = df.dropna(subset=['action_id'])

        if df.empty:
            print("No valid records found after mapping.")
            return

        # Generate fields
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Name and description: Set default if empty, truncated to 191 chars
        df['name'] = df['tipologia_assistencia'].fillna("Assistència Tècnica").astype(str).str.slice(0, 191)
        df['description'] = df['tipologia_assistencia'].fillna("").astype(str).str.slice(0, 191)
        
        # Map code and hardcode type
        df['code'] = df['codi_tipologia'] 
        df['type'] = 'technical-assistance'
        
        # Parse amount
        df['amount'] = pd.to_numeric(df['import_total'], errors='coerce').fillna(0)
        
        # Project ID: Left as NULL for now
        df['project_id'] = None

        # Timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        # Final column selection
        final_cols = ['id', 'name', 'description', 'action_id', 'project_id', 
                      'code', 'type', 'amount', 'created_at', 'updated_at']
        df = df[final_cols]
        
    else:
        print("Postgres returned no data.")
        return

    # Insert data into MySQL
    print("Inserting into economic_items table...")
    try:
        # Use append to add to existing records
        df.to_sql('economic_items', con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df)} items inserted.")
        
    except Exception as e:
        print(f"Insertion error: {e}")


# OBRA:
# prové de les dades de la nostra base de dades, de la taula dadeseconomiques_obra
# name = "Obra"
# code = "Obra"
# desciption = "Obra"
# type = "work"
# amount = pec_iva
# action_id = referència a la taula actions, la taula antiga té cas que fa referència al tracking_code de la taula actions, per tant haurem de fer un mapping amb el camp cas de dadeseconomiques_obra com a tracking_code per trobar el uuid de l'acció
# project_id = DE MOMENT NO RELACIONEM, ELLS NO HO FAN (cercar mateix action_id i mateix codi_tipologia a la taula projects, per trobar el projecte que té aquesta assistència tècnica (a la taula projects, el camp code és el codi_tipologia i el camp action_id és el mateix que l'acció de l'economic_item))

# !!!! M'he trobat que hi ha obres duplicades, s'ha DE REVISAAAAR, però ara mateix em quedaré amb la última fila que apareix a Postgres

def migrate_economic_items_obra(mysql_url):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting Works Economic Items migration (No Duplicates)...")

    # Load dependencies
    print("Loading Actions map...")
    try:
        actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        action_map = dict(zip(actions_df['tracking_code'], actions_df['id']))
    except Exception as e:
        print(f"MySQL dependencies error: {e}")
        return

    # Read data from Postgres
    print("Reading from Postgres table dadeseconomiques_obra...")
    df = pd.DataFrame()
    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        query = "SELECT cas, pec_iva FROM dadeseconomiques_obra"
        df = pd.read_sql(query, pg_conn)
        print(f"Read {len(df)} rows.")
    except Exception as e:
        print(f"Error reading Postgres: {e}")
        return
    finally:
        if pg_conn: pg_conn.close()

    # Data transformation
    if not df.empty:
        print("Transforming and deduplicating data...")
        
        # Clean tracking code
        df['clean_code'] = pd.to_numeric(df['cas'], errors='coerce').fillna(0).astype(int).astype(str)
        
        # Map action ID
        df['action_id'] = df['clean_code'].map(action_map)
        
        # Filter out orphan records
        df = df.dropna(subset=['action_id'])

        if df.empty:
            print("No valid items found after mapping.")
            return

        # Deduplicate by action_id, keeping the last occurrence
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['action_id'], keep='last')
        after_dedup = len(df)
        
        if before_dedup > after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicate records.")

        # Generate fields
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        df['name'] = "Obra"
        df['description'] = "Obra"
        df['code'] = "Obra" 
        df['type'] = 'work'
        
        df['amount'] = pd.to_numeric(df['pec_iva'], errors='coerce').fillna(0)
        df['project_id'] = None

        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        final_cols = ['id', 'name', 'description', 'action_id', 'project_id', 
                      'code', 'type', 'amount', 'created_at', 'updated_at']
        df = df[final_cols]
        
    else:
        print("Postgres returned no data.")
        return

    # Insert data into MySQL
    print("Inserting into economic_items table...")
    try:
        df.to_sql('economic_items', con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df)} items inserted.")
    except Exception as e:
        print(f"Insertion error: {e}")
        if "Duplicate entry" in str(e):
             print("Warning: Possible duplicate entry error on 'code' field.")


# migrar economic_items_anual_budgets:
# AT
# prové de les dades de la nostra base de dades, de la taula dadeseconomiques_at_financament_anualitats
# econoomic_item_id, referència a economic_items, hauré de cercar el projecte i actuacio a la que pertany i cercar aquest parell a economic_items per trobar el id
# en aquesta taula tenim id_at que es del tipus: XXXX_ATY, on XXXX és el codi de l'actuació i Y és el número de AT, per tant per trobar el economic_item_id hauré de fer un mapping amb el camp id_at de dadeseconomiques_at_financament i el camp code d'economic_items
# amount = import_anualitat
# year = anualitat
# Saltar alertes si la suma de les anualitats no coincideix amb el amount de economic_items, i printejar per pantalla quin cas es i AT.

def migrate_economic_items_anual_budgets_AT(mysql_url):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting AT Annual Budgets migration...")

    # Load reference maps from MySQL
    print("Loading dependencies...")
    try:
        actions_df = pd.read_sql("SELECT id as action_id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        
        items_query = """
            SELECT id as item_uuid, action_id, code as item_code, amount as total_item_amount 
            FROM economic_items 
            WHERE type = 'technical-assistance' AND code IS NOT NULL
        """
        items_df = pd.read_sql(items_query, mysql_engine)

        # Merge Items with Actions to map tracking_code for cleaner output
        items_enriched = items_df.merge(actions_df, on='action_id', how='left')

        print(f"Loaded {len(actions_df)} Actions and {len(items_df)} Items.")

    except Exception as e:
        print(f"Error loading dependencies: {e}")
        return

    # Read data from Postgres
    print("Reading Postgres table dadeseconomiques_at_financament_anualitats...")
    df = pd.DataFrame()
    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        query = "SELECT id_at, anualitat, import_anualitat FROM dadeseconomiques_at_financament_anualitats" 
        df = pd.read_sql(query, pg_conn)
        print(f"Read {len(df)} rows.")
    except Exception as e:
        print(f"Postgres error: {e}")
        return
    finally:
        if pg_conn: pg_conn.close()

    if df.empty: return

    # Data processing and mapping
    print("Processing IDs...")

    # Extract tracking code and item code from id_at (e.g., "751_AT1")
    split_data = df['id_at'].str.extract(r'^([^_]+)_(.+)$')
    df['pg_tracking_code'] = split_data[0]
    df['pg_item_code'] = split_data[1]

    df['budget_amount'] = pd.to_numeric(df['import_anualitat'], errors='coerce').fillna(0)
    df['year'] = pd.to_numeric(df['anualitat'], errors='coerce').fillna(0).astype(int)

    # Map ACTION_ID
    df = df.merge(actions_df, left_on='pg_tracking_code', right_on='tracking_code', how='left')
    df = df.dropna(subset=['action_id'])

    # Map ITEM_ID
    df = df.merge(items_df, left_on=['action_id', 'pg_item_code'], right_on=['action_id', 'item_code'], how='left')
    
    # Keep only fully mapped valid rows
    df = df.dropna(subset=['item_uuid'])

    print(f"Valid rows ready for processing: {len(df)}")

    # Validation Alerts

    # Alert 1: Items with a budget but no annual breakdown
    print("\n[Validation] Checking for items with budget but no annual breakdown...")
    
    uuids_with_annuals = set(df['item_uuid'].unique())
    
    ghost_items = items_enriched[
        (~items_enriched['item_uuid'].isin(uuids_with_annuals)) & 
        (items_enriched['total_item_amount'] > 0.01)
    ].copy()
    
    if not ghost_items.empty:
        print(f"CRITICAL ALERT: {len(ghost_items)} Items have a budget but no corresponding annual breakdown:")
        print("-" * 60)
        print(f"{'ACTION (Tracking)':<20} | {'ITEM CODE':<15} | {'BUDGET (€)':<20}")
        print("-" * 60)
        
        ghost_items = ghost_items.sort_values(by='total_item_amount', ascending=False)
        
        for _, row in ghost_items.iterrows():
            track = str(row['tracking_code'])
            code = str(row['item_code'])
            amount = row['total_item_amount']
            print(f"{track:<20} | {code:<15} | {amount:<20.2f}")
            
        print("-" * 60)
        print("Note: These items will remain in the DB with their total budget, but lacking annual breakdown.")
    else:
        print("Check passed: No orphaned items with a budget found.")

    # Alert 2: Annual sums mismatching total item amount
    print("\n[Validation] Verifying that annual sums match the total item budget...")
    validation = df.groupby('item_uuid').agg({
        'budget_amount': 'sum',
        'total_item_amount': 'first',
        'id_at': 'first'
    }).reset_index()

    sum_alerts = []
    for _, row in validation.iterrows():
        annual_sum = row['budget_amount']
        total_budget = row['total_item_amount']
        if not np.isclose(annual_sum, total_budget, atol=0.05):
            sum_alerts.append([row['id_at'], annual_sum, total_budget, annual_sum - total_budget])

    sum_alerts.sort(key=lambda x: abs(x[3]), reverse=True)

    if sum_alerts:
        print(f"ALERT: {len(sum_alerts)} Items where the annual sum does not match the total:")
        print(f"{'ID_AT':<20} | {'ANNUAL SUM':<15} | {'TOTAL ITEM':<15} | {'DIFFERENCE':<10}")
        print("-" * 65)
        for alert in sum_alerts:
            print(f"{alert[0]:<20} | {alert[1]:<15.2f} | {alert[2]:<15.2f} | {alert[3]:<10.2f}")
        print("-" * 65)
    else:
        print("Check passed: All sums match correctly.")

    # Data Insertion
    if df.empty:
        print("\nNo data available to insert.")
        return

    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['created_at'] = datetime.now()
    df['updated_at'] = datetime.now()
    df['economic_item_id'] = df['item_uuid']
    df['amount'] = df['budget_amount']

    final_cols = ['id', 'economic_item_id', 'amount', 'year', 'created_at', 'updated_at']
    df_final = df[final_cols]

    print("\nInserting data into 'economic_item_anual_budgets'...")
    try:
        table_name = 'economic_item_anual_budgets'
        df_final.to_sql(table_name, con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df_final)} rows inserted.")
    except Exception as e:
        print(f"Insertion error: {e}")


# Obra:
# prové de les dades de la nostra base de dades, de la taula dadeseconomiques_obra_financament
# econoomic_item_id, referència a economic_items, hauré de cercar el projecte i actuacio a la que pertany i cercar aquest parell a economic_items per trobar el id
# en aquesta taula tenim cas que es del tipus: XXXX, on XXXX és el codi de l'actuació, per tant per trobar el economic_item_id hauré de fer un mapping de l'actuació que li toca i que a name fiqui "Obra"
# amount = import_anualitat
# year = anualitat

def migrate_economic_items_anual_budgets_obra(mysql_url, oracle_config):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting Works Annual Budgets migration...")

    # Load reference maps from MySQL
    print("Loading dependencies...")
    try:
        actions_df = pd.read_sql("SELECT id as action_id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        
        # Filter for 'work' items or items named 'Obra'
        items_query = """
            SELECT id as item_uuid, action_id, code as item_code, amount as total_item_amount 
            FROM economic_items 
            WHERE type = 'work' OR name = 'Obra'
        """
        items_df = pd.read_sql(items_query, mysql_engine)

        # Merge with actions to make alerts readable by tracking code
        items_enriched = items_df.merge(actions_df, on='action_id', how='left')

        print(f"Loaded {len(actions_df)} Actions and {len(items_df)} Works Items.")

    except Exception as e:
        print(f"Error loading dependencies: {e}")
        return

    # Read data from Postgres
    print("Reading from Postgres table dadeseconomiques_obra_financament...")
    df = pd.DataFrame()
    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        query = "SELECT cas, anualitat, import_anualitat FROM dadeseconomiques_obra_financament" 
        df = pd.read_sql(query, pg_conn)
        print(f"Read {len(df)} rows.")
    except Exception as e:
        print(f"Postgres error: {e}")
        return
    finally:
        if pg_conn: pg_conn.close()

    if df.empty: return

    # Data processing and mapping
    print("Processing IDs...")

    # Extract tracking code
    df['pg_tracking_code'] = pd.to_numeric(df['cas'], errors='coerce').fillna(0).astype(int).astype(str)
    
    df['budget_amount'] = pd.to_numeric(df['import_anualitat'], errors='coerce').fillna(0)
    df['year'] = pd.to_numeric(df['anualitat'], errors='coerce').fillna(0).astype(int)

    # Map ACTION_ID
    df = df.merge(actions_df, left_on='pg_tracking_code', right_on='tracking_code', how='left')
    
    df = df.dropna(subset=['action_id'])

    # Map ITEM_ID using action_id
    df = df.merge(items_df, on='action_id', how='left')
    
    # Keep valid rows
    df = df.dropna(subset=['item_uuid'])

    print(f"Valid rows ready for processing: {len(df)}")

    # Validation Alerts

    # Alert 1: Works items with a budget but no annual breakdown
    print("\n[Validation] Checking for Works items with budget but no annual breakdown...")
    
    uuids_with_annuals = set(df['item_uuid'].unique())
    
    ghost_items = items_enriched[
        (~items_enriched['item_uuid'].isin(uuids_with_annuals)) & 
        (items_enriched['total_item_amount'] > 0.01)
    ].copy()
    
    if not ghost_items.empty:
        print(f"CRITICAL ALERT: {len(ghost_items)} Works items have a budget but no corresponding annual breakdown:")
        print("-" * 60)
        print(f"{'ACTION (Tracking)':<20} | {'BUDGET (€)':<20}")
        print("-" * 60)
        
        ghost_items = ghost_items.sort_values(by='total_item_amount', ascending=False)
        for _, row in ghost_items.iterrows():
            track = str(row['tracking_code'])
            amount = row['total_item_amount']
            print(f"{track:<20} | {amount:<20.2f}")
            
        print("-" * 60)
    else:
        print("Check passed: No orphaned Works items with a budget found.")

    # Alert 2: Annual sums mismatching total Works item amount
    print("\n[Validation] Verifying that annual sums match the total Works budget...")
    validation = df.groupby('item_uuid').agg({
        'budget_amount': 'sum',
        'total_item_amount': 'first',
        'pg_tracking_code': 'first'
    }).reset_index()

    sum_alerts = []
    for _, row in validation.iterrows():
        annual_sum = row['budget_amount']
        total_budget = row['total_item_amount']
        if not np.isclose(annual_sum, total_budget, atol=0.05):
            sum_alerts.append([row['pg_tracking_code'], annual_sum, total_budget, annual_sum - total_budget])

    sum_alerts.sort(key=lambda x: abs(x[3]), reverse=True)

    if sum_alerts:
        print(f"ALERT: {len(sum_alerts)} Works items where the annual sum does not match the total:")
        print(f"{'CASE':<10} | {'ANNUAL SUM':<15} | {'TOTAL WORKS':<15} | {'DIFFERENCE':<10}")
        print("-" * 55)
        for alert in sum_alerts:
            print(f"{alert[0]:<10} | {alert[1]:<15.2f} | {alert[2]:<15.2f} | {alert[3]:<10.2f}")
        print("-" * 55)
    else:
        print("Check passed: All sums match correctly.")

    # Data Insertion
    if df.empty:
        print("\nNo data available to insert.")
        return

    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['created_at'] = datetime.now()
    df['updated_at'] = datetime.now()
    
    df['economic_item_id'] = df['item_uuid']
    df['amount'] = df['budget_amount']

    final_cols = ['id', 'economic_item_id', 'amount', 'year', 'created_at', 'updated_at']
    df_final = df[final_cols]

    print("\nInserting data into 'economic_item_anual_budgets'...")
    try:
        table_name = 'economic_item_anual_budgets'
        df_final.to_sql(table_name, con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df_final)} rows inserted.")
    except Exception as e:
        print(f"Insertion error: {e}")