from datetime import datetime
import uuid
from flask import json
import pandas as pd
import oracledb
from sqlalchemy import create_engine


#AT
# Les dades provenen de PIO_FACTURACIOX on X, s'haurà d'interar de l'1 al 20.
# Per mirar a quin project correpont hem de fer el següent:
# A la taula PIO_FACTURACIOX mirar el camp PROJECTE i relacionar aquest camp amb el camp PROJECTE de la taula PIO_ACTUACIONS, així trobem el tracking_code que es el camp CASEID a la taula PIO_ACTUACIONS
# Un cop tenim el tracking_code, el relacionem amb la taula actions per trobar el id de l'acció
# Un cop tenim el id de l'acció, cerquem a la taula projects un projecte que tingui aquest action_id i tingui com a name "ATX", on X és l'int de l'AT i ens quedem amb el project_id
# project_id = referència a la taula projects
# month = Mirem si a concepte surt un mes i ens quedem amb el primer que aparegui, si no surt cap, el deixem a null
# net_amount = IMPORTFACTURAR
# amount = IMPORTFACTURAR * 0.79 (Sense IVA) 
# certification_date = DATAFACTURA
# Per relacionar la certificació amb l'AT fem un JOIN amb PIO_TASQUES
# A PIO_TASQUES tenim uns strings que fem mapping en un json amb les ATX.

def migrate_certifications_AT(mysql_url, oracle_config, json_file):
    print("Starting AT Certifications migration...")
    
    mysql_engine = create_engine(mysql_url)

    # Prepare reverse JSON map (Task -> ATX)
    tasca_to_at_map = {}
    with open(json_file, 'r', encoding='utf-8') as f:
        json_at = json.load(f)
        
    for at_key, tasques in json_at.items():
        for t in tasques:
            if t not in tasca_to_at_map:
                tasca_to_at_map[t] = at_key

    # Load MySQL context
    print("Loading MySQL maps (Actions and Projects)...")
    try:
        actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
        actions_df['clean_code'] = pd.to_numeric(actions_df['tracking_code'], errors='coerce').fillna(0).astype(int).astype(str)
        action_map = dict(zip(actions_df['clean_code'], actions_df['id']))

        proj_df = pd.read_sql("SELECT id, action_id, code FROM projects", mysql_engine)
        proj_df['code_clean'] = proj_df['code'].astype(str).str.strip()
        project_lookup = dict(zip(zip(proj_df['action_id'], proj_df['code_clean']), proj_df['id']))

    except Exception as e:
        print(f"Error loading MySQL dependencies: {e}")
        return

    # Oracle Connection and Processing
    try:
        with oracledb.connect(**oracle_config) as ora_conn:
            print("Connected to Oracle.")

            q_bridge = "SELECT PROJECTE, CASEID FROM PIO_ACTUACIONS"
            bridge_df = pd.read_sql(q_bridge, ora_conn)
            bridge_df['proj_clean'] = pd.to_numeric(bridge_df['PROJECTE'], errors='coerce').fillna(0).astype(int).astype(str)
            bridge_df['case_clean'] = pd.to_numeric(bridge_df['CASEID'], errors='coerce').fillna(0).astype(int).astype(str)
            oracle_bridge_map = dict(zip(bridge_df['proj_clean'], bridge_df['case_clean']))
            
            total_migrated = 0

            # Iterate through billing tables
            for i in range(1, 21):
                table_name = f"PIO_FACTURACIO{i}"
                if i == 3:
                    table_name = "FACTURACIO3"
                    
                print(f"Processing {table_name}...")

                try:
                    query = f"""
                        SELECT 
                            F.PROJECTE, 
                            F.IMPORTFACTURAR AS IMPORT_BASE, 
                            F.DATAFACTURA, 
                            F.CONCEPTE,
                            T.NOMTASCA
                        FROM {table_name} F
                        LEFT JOIN PIO_PROJECTE P ON F.PROJECTE = P.IDPROJECTE
                        LEFT JOIN PIO_TASQUES T ON P.TASCAFACTURACIO{i} = T.IDTASQUES
                    """
                    df_fact = pd.read_sql(query, ora_conn)
                except Exception:
                    try:
                        query = f"""
                            SELECT 
                                F.PROJECTE, 
                                F.IMPORTFACTURAT AS IMPORT_BASE, 
                                F.DATAFACTURA, 
                                F.CONCEPTE,
                                T.NOMTASCA
                            FROM {table_name} F
                            LEFT JOIN PIO_PROJECTE P ON F.PROJECTE = P.IDPROJECTE
                            LEFT JOIN PIO_TASQUES T ON P.TASCAFACTURACIO{i} = T.IDTASQUES
                        """
                        df_fact = pd.read_sql(query, ora_conn)
                    except Exception:
                        try:
                            query = f"""
                                SELECT 
                                    F.PROJECTE, 
                                    F.IMPORTFACTURA AS IMPORT_BASE, 
                                    F.DATAFACTURA, 
                                    F.CONCEPTE,
                                    T.NOMTASCA
                                FROM {table_name} F
                                LEFT JOIN PIO_PROJECTE P ON F.PROJECTE = P.IDPROJECTE
                                LEFT JOIN PIO_TASQUES T ON P.TASCAFACTURACIO{i} = T.IDTASQUES
                            """
                            df_fact = pd.read_sql(query, ora_conn)
                        except Exception:
                            try:
                                query = f"""
                                    SELECT 
                                        F.PROJECTE, 
                                        F.IMPORTFACTURAT AS IMPORT_BASE, 
                                        F.DATAFACTURACIO AS DATAFACTURA, 
                                        F.CONCEPTE,
                                        T.NOMTASCA
                                    FROM {table_name} F
                                    LEFT JOIN PIO_PROJECTE P ON F.PROJECTE = P.IDPROJECTE
                                    LEFT JOIN PIO_TASQUES T ON P.TASCAFACTURACIO{i} = T.IDTASQUES
                                """
                                df_fact = pd.read_sql(query, ora_conn)
                            except Exception:
                                print(f"Could not read {table_name}. Skipping.")
                                continue

                if df_fact.empty:
                    print(f"Table {table_name} is empty.")
                    continue

                # ID cleaning and mapping
                df_fact['proj_ora_clean'] = pd.to_numeric(df_fact['PROJECTE'], errors='coerce').fillna(0).astype(int).astype(str)
                df_fact['nomtasca_clean'] = df_fact['NOMTASCA'].fillna("").astype(str).str.strip()

                df_fact['tracking_code'] = df_fact['proj_ora_clean'].map(oracle_bridge_map)
                df_fact['action_id'] = df_fact['tracking_code'].map(action_map)
                df_fact['target_project_name'] = df_fact['nomtasca_clean'].map(tasca_to_at_map)

                def find_project_id(row):
                    if pd.isna(row['action_id']) or pd.isna(row['target_project_name']): 
                        return None
                    key = (row['action_id'], row['target_project_name'])
                    return project_lookup.get(key)

                df_fact['project_id'] = df_fact.apply(find_project_id, axis=1)

                # Filter valid projects
                df_final = df_fact.dropna(subset=['project_id']).copy()

                if df_final.empty:
                    print("No valid invoices to cross-reference.")
                    continue

                # Extract month
                month_pattern = r'(?i)\b(gener|febrer|març|abril|maig|juny|juliol|agost|setembre|octubre|novembre|desembre)\b'
                df_final['CONCEPTE_STR'] = df_final['CONCEPTE'].fillna("").astype(str)
                df_final['month'] = df_final['CONCEPTE_STR'].str.extract(month_pattern, expand=False).str.lower()

                # Generate final fields
                df_final['id'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
                df_final['net_amount'] = df_final['IMPORT_BASE']
                df_final['amount'] = df_final['IMPORT_BASE'] * 0.79 
                df_final['certification_date'] = pd.to_datetime(df_final['DATAFACTURA'])
                df_final['created_at'] = datetime.now()
                df_final['updated_at'] = datetime.now()
                
                cols_to_insert = [
                    'id', 'project_id', 'month', 'amount', 'net_amount', 
                    'certification_date', 'created_at', 'updated_at'
                ]
                
                # Insert into MySQL
                df_final[cols_to_insert].to_sql('certifications', con=mysql_engine, if_exists='append', index=False)
                
                count = len(df_final)
                total_migrated += count
                print(f"Inserted {count} dynamically assigned certifications.")

            print(f"\nMIGRATION COMPLETED. Total certifications: {total_migrated}")

    except Exception as e:
        print(f"Fatal Error: {e}")



# migrar certifications:
# Obra:
# prové de PIO. PIO_CERTIFICACIONS
# Per mirar a quin project correpont hem de fer el següent:
# A la taula PIO_CERTIFICACIONS mirar el camp OBRA i relacionar aquest camp amb el camp OBRA de la taula PIO_ACTUACIONS, així trobem el tracking_code que es el camp CASEID a la taula PIO_ACTUACIONS
# Un cop tenim el tracking_code, el relacionem amb la taula actions per trobar el id de l'acció
# Un cop tenim el id de l'acció, cerquem a la taula projects un projecte que tingui aquest action_id i tingui com a name "Obra" i ens quedem amb el project_id
# project_id = referència a la taula projects
# month = MESCERTIFICACIO
# net_amount = IMPORTAMBIVA
# amount = IMPORTAMBIVA * 0.79 (Sense IVA)
# certification_date = DATACERTIFICACIO

def migrate_certifications_obra(mysql_url, oracle_config):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting Certifications migration (Oracle PIO -> MySQL)...")

    # Load MySQL dependencies
    try:
        actions_df = pd.read_sql("SELECT id as action_id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        
        proj_query = "SELECT id as project_id, action_id FROM projects WHERE name = 'Obra'"
        projects_df = pd.read_sql(proj_query, mysql_engine)

    except Exception as e:
        print(f"Error loading MySQL dependencies: {e}")
        return

    # Read from Oracle
    print("Connecting to Oracle and reading tables...")
    df_cert = pd.DataFrame()
    df_act = pd.DataFrame()
    
    try:
        with oracledb.connect(**oracle_config) as ora_conn:
            q_cert = "SELECT OBRA, MESCERTIFICACIO, IMPORTAMBIVA, DATACERTIFICACIO FROM PIO_CERTIFICACIONS"
            df_cert = pd.read_sql(q_cert, ora_conn)

            q_act = "SELECT OBRA, CASEID FROM PIO_ACTUACIONS"
            df_act = pd.read_sql(q_act, ora_conn)

    except Exception as e:
        print(f"Oracle connection error: {e}")
        return

    if df_cert.empty: return

    # Cross-reference data
    print("Cross-referencing data...")

    merged_pio = df_cert.merge(df_act, on='OBRA', how='left')
    merged_pio['clean_tracking'] = merged_pio['CASEID'].astype(str).str.strip()
    
    merged_pio = merged_pio.merge(actions_df, left_on='clean_tracking', right_on='tracking_code', how='left')
    merged_pio = merged_pio.dropna(subset=['action_id'])

    final_df = merged_pio.merge(projects_df, on='action_id', how='left')
    final_df = final_df.dropna(subset=['project_id'])

    if final_df.empty: 
        print("No valid rows to insert.")
        return

    # Print summary of tracking codes
    print("\nSUMMARY OF ACTIONS TO BE INSERTED:")
    print(f"{'TRACKING CODE':<20} | {'CERTIFICATIONS COUNT':<20}")
    print("-" * 45)
    
    summary = final_df['clean_tracking'].value_counts().reset_index()
    summary.columns = ['code', 'count']
    
    for index, row in summary.iterrows():
        print(f"{row['code']:<20} | {row['count']:<20}")
    
    print("-" * 45)
    print(f"TOTAL UNIQUE ACTIONS: {len(summary)}\n")

    # Final data transformation
    final_df['id'] = [str(uuid.uuid4()) for _ in range(len(final_df))]
    month_pattern = r'(?i)\b(gener|febrer|març|abril|maig|juny|juliol|agost|setembre|octubre|novembre|desembre)\b'
    
    final_df['MESCERTIFICACIO_STR'] = final_df['MESCERTIFICACIO'].fillna("").astype(str)
    
    final_df['month'] = final_df['MESCERTIFICACIO_STR'].str.extract(month_pattern, expand=False).str.lower()
    final_df['net_amount'] = pd.to_numeric(final_df['IMPORTAMBIVA'], errors='coerce').fillna(0)
    final_df['amount'] = final_df['net_amount'] / 0.79
    final_df['certification_date'] = pd.to_datetime(final_df['DATACERTIFICACIO'], errors='coerce')
    final_df['created_at'] = datetime.now()
    final_df['updated_at'] = datetime.now()

    cols_to_insert = ['id', 'project_id', 'month', 'amount', 'net_amount', 'certification_date', 'created_at', 'updated_at']
    df_upload = final_df[cols_to_insert]

    # Insert into MySQL
    print("Inserting into 'certifications' table...")
    try:
        df_upload.to_sql('certifications', con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df_upload)} certifications inserted.")
    except Exception as e:
        print(f"Insertion error: {e}")