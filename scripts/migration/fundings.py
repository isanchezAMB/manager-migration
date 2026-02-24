from datetime import datetime
import oracledb
from sqlalchemy import create_engine
import pandas as pd
import uuid

# Migrar financament:
# FUNDING_TYPES = PIO_TIPUSFINANCAMENT
# key = FINANCAMENT
# initial = FINANCAMENT
# FUNDINGS = PIO_FINANCAMENT
# initial_amount = IMPORTPREVIST
# initial_percentage = PERCPREVISTRESPECTETOTAL
# real_amount = IMPORTREAL
# real_percentage = PERCREALRESPECTETOTAL
# funding_type_id = referència a FUNDING_TYPES, per tant haurem de fer un mapping amb el camp FINANCAMENT de FUNDINGS com abans amb els serveis per les accions
# actions_id = referència a la taula actions, la taula antiga té ACTUACIONS, que fa referència a tracking_code de la taula actions,llavors haurem d'aconseguir el id de la taula actions a través del tracking_code 


def migrate_fundings(mysql_url, oracle_config):
    print("Starting Fundings migration...")
    mysql_engine = create_engine(mysql_url)
    
    try:
        with oracledb.connect(
            user=oracle_config["user"], 
            password=oracle_config["password"], 
            dsn=oracle_config["dsn"]
        ) as ora_conn:
            
            # MIGRATE FUNDING TYPES
            print("Migrating funding types catalog...")
            query_types = "SELECT DISTINCT FINANCAMENT FROM PIO_TIPUSFINANCAMENT WHERE FINANCAMENT IS NOT NULL"
            types_raw = pd.read_sql(query_types, ora_conn)
            
            if not types_raw.empty:
                df_types = pd.DataFrame()
                df_types['id'] = [str(uuid.uuid4()) for _ in range(len(types_raw))]
                df_types['key'] = types_raw['FINANCAMENT'].astype(str).str.strip()
                df_types['initial'] = df_types['key']
                df_types['created_at'] = datetime.now()
                df_types['updated_at'] = datetime.now()
                
                df_types.to_sql('funding_types', con=mysql_engine, if_exists='append', index=False)
                print(f"Migrated {len(df_types)} funding types.")

            # MAP AND MIGRATE FUNDINGS
            print("Mapping and migrating fundings...")
            
            # Load normalized mappings from MySQL
            ft_df = pd.read_sql("SELECT id, `key` FROM funding_types", mysql_engine)
            ft_map = {str(k).strip().upper(): v for k, v in zip(ft_df['key'], ft_df['id'])}

            actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
            actions_map = {str(k).strip().upper(): v for k, v in zip(actions_df['tracking_code'], actions_df['id'])}

            # Read fundings from Oracle
            query_fundings = """
                SELECT 
                    f.IMPORTPREVIST, f.PERCPREVISTRESPECTETOTAL,
                    f.IMPORTREAL, f.PERCREALRESPECTETOTAL,
                    f.ACTUACIONS,
                    t.FINANCAMENT as TYPE_KEY
                FROM PIO_FINANCAMENT f
                LEFT JOIN PIO_TIPUSFINANCAMENT t ON f.TIPUSFINANCAMENT = t.IDTIPUSFINANCAMENT
            """
            df_fundings_raw = pd.read_sql(query_fundings, ora_conn)

            if not df_fundings_raw.empty:
                df_final = pd.DataFrame(index=df_fundings_raw.index)
                df_final['id'] = [str(uuid.uuid4()) for _ in range(len(df_fundings_raw))]
                
                # Normalize Oracle search columns
                raw_type_key = df_fundings_raw['TYPE_KEY'].astype(str).str.strip().str.upper()
                raw_action_key = df_fundings_raw['ACTUACIONS'].astype("Int64").astype(str).str.strip().str.upper()

                # Map keys to UUIDs
                df_final['funding_type_id'] = raw_type_key.map(ft_map)
                df_final['action_id'] = raw_action_key.map(actions_map)
                
                # Map financial data
                df_final['initial_amount'] = df_fundings_raw['IMPORTPREVIST']
                df_final['initial_percentage'] = df_fundings_raw['PERCPREVISTRESPECTETOTAL']
                df_final['real_amount'] = df_fundings_raw['IMPORTREAL']
                df_final['real_percentage'] = df_fundings_raw['PERCREALRESPECTETOTAL']
                df_final['created_at'] = datetime.now()
                df_final['updated_at'] = datetime.now()

                # Filter out orphan records
                df_final = df_final.dropna(subset=['action_id'])
                
                if not df_final.empty:
                    df_final.to_sql('fundings', con=mysql_engine, if_exists='append', index=False)
                    print(f"Success! {len(df_final)} fundings inserted.")
                else:
                    print("No valid fundings found to insert after mapping.")

    except Exception as e:
        print(f"Fatal Error: {e}")