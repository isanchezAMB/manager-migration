import pandas as pd
from sqlalchemy import create_engine
import oracledb
import uuid
from datetime import datetime

def migrate_programs(mysql_url, oracle_config):
    print("Starting Programs migration...")
    mysql_engine = create_engine(mysql_url)

    try:
        with oracledb.connect(**oracle_config) as ora_conn:
            
            # STEP 1: MIGRATE PROGRAMS CATALOG (Destination: 'programs')
            print("Migrating programs catalog...")
            
            query_types = "SELECT DISTINCT FINANCAMENT FROM PIO_TIPUSFINANCAMENT WHERE FINANCAMENT IS NOT NULL"
            types_raw = pd.read_sql(query_types, ora_conn)
            
            if not types_raw.empty:
                df_programs = pd.DataFrame()
                df_programs['id'] = [str(uuid.uuid4()) for _ in range(len(types_raw))]
                df_programs['key'] = types_raw['FINANCAMENT'].astype(str).str.strip()
                df_programs['name'] = df_programs['key']
                df_programs['created_at'] = datetime.now()
                df_programs['updated_at'] = datetime.now()
                
                df_programs.to_sql('programs', con=mysql_engine, if_exists='append', index=False)
                print(f"Migrated {len(df_programs)} programs into catalog.")

            # STEP 2: MIGRATE PIVOT TABLE (Destination: 'action_program')
            print("Mapping Actions to Programs...")
            
            # Load the newly inserted programs mapping
            prog_df = pd.read_sql("SELECT id, `key` FROM programs", mysql_engine)
            prog_map = {str(k).strip().upper(): v for k, v in zip(prog_df['key'], prog_df['id'])}

            # Load the actions mapping
            actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
            actions_map = {str(k).strip().upper(): v for k, v in zip(actions_df['tracking_code'], actions_df['id'])}

            # Read the relationships from Oracle
            query_relations = """
                SELECT 
                    f.ACTUACIONS, 
                    t.FINANCAMENT as TYPE_KEY
                FROM PIO_FINANCAMENT f
                LEFT JOIN PIO_TIPUSFINANCAMENT t ON f.TIPUSFINANCAMENT = t.IDTIPUSFINANCAMENT
            """
            df_relations_raw = pd.read_sql(query_relations, ora_conn)

            if not df_relations_raw.empty:
                df_final = pd.DataFrame(index=df_relations_raw.index)
                
                # Normalize keys to match the mappings
                raw_type_key = df_relations_raw['TYPE_KEY'].astype(str).str.strip().str.upper()
                raw_action_key = pd.to_numeric(df_relations_raw['ACTUACIONS'], errors='coerce').fillna(0).astype(int).astype(str).str.upper()

                # Map string keys to UUIDs
                df_final['program_id'] = raw_type_key.map(prog_map)
                df_final['action_id'] = raw_action_key.map(actions_map)
                
                df_final['created_at'] = datetime.now()
                df_final['updated_at'] = datetime.now()

                # Filter out orphan records and exact duplicates
                df_final = df_final.dropna(subset=['action_id', 'program_id'])
                df_final = df_final.drop_duplicates(subset=['action_id', 'program_id'])
                
                if not df_final.empty:
                    df_final.to_sql('action_program', con=mysql_engine, if_exists='append', index=False)
                    print(f"Success! {len(df_final)} relations inserted into 'action_program'.")
                else:
                    print("No valid relations found to insert after mapping.")

    except Exception as e:
        print(f"Fatal Error: {e}")