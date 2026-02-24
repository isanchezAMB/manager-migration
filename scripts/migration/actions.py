from datetime import datetime
import uuid
from sqlalchemy import create_engine, text
import pandas as pd
import oracledb
from flask import json



# Migrar actions:
# La majoria d'info prové de PIO_ACTUACIONS (gestor antic)
# tracking_code = CASEID
# name = TITOL
# alias = ALIES
# description = DESCRIPCIOPROJECTE (és d'una altra taula PIO_PROJECTE)
# owner_id, s'haura de fer a mà suposo
# service_id = SERVEIPO (referència a IDSERVEI_PO de la taula PIO_SERVEI_PO) (Hauria de ser el id de la taula services corresponent)
# action_type_id, fet a update_types (falta canviar el json)
# action_subtype_id, fet a update_types (falta canviar el json)
# geometry, json de shapes
# geometry_type, si està dibuixat o upload
# geometry_source, si està dibuixat o upload (en català)
# address = ADRECA

# NO TENIM INFO
# assistance_type_id
# priority_id (referencia a taula priorities)
# requested_at
# request_channel_id (referencia a taula request_channels)
# phase_0_finished_at
# phase_1_finished_at
# phase_2_finished_at
# phase_3_finished_at
# final_finished_at


# NO SABEM QUE ÉS
# activity_type_id
# status
# subsidy_pending
# subsidy_notes
# accounting_project_code (sembla que es expedient de proj)
# started_at
# ended_at
# holded_at
# programme_membership_data
# target_date_p0
# target_date_p1
# target_date_p2
# objectives_description
# area (suposo que és l'area en m2, però no ho sé segur)
# comments



# NO MIGRAR, NO CRITIC, ES FARAN AL GESTOR MANUALMENT SOTA DEMANDA
# phase_0_questionary
# phase_1_questionary
# phase_2_questionary
# phase_3_questionary
# final_questionary
# phase_0_feedback
# feedback_token
# feedback_emails
# has_.... (molt camps de booleanos que no tenim info)
# uploaded_documents



def migrate_actions(mysql_url, oracle_config, valid_cases):
    mysql_engine = create_engine(mysql_url)
    # Fetch UUID mapping from MySQL
    print("Fetching service UUIDs from MySQL...")
    try:
        services_df = pd.read_sql("SELECT id, `key` FROM services", mysql_engine)
        services_df['key'] = services_df['key'].astype(str).str.strip()
        service_map = dict(zip(services_df['key'], services_df['id']))
    except Exception as e:
        print(f"Error loading services: {e}")
        return

    # Extract Data from Oracle
    print("Connecting to Oracle...")
    df_raw = pd.DataFrame()
    try:
        with oracledb.connect(user=oracle_config["user"], 
                              password=oracle_config["password"], 
                              dsn=oracle_config["dsn"]) as ora_conn:
            
            query = """
                    SELECT 
                        a.CASEID, a.TITOL, a.ALIES, a.ADRECA,
                        p.DESCRIPCIOPROJECTE,
                        s.CODI as SERVICE_KEY
                    FROM PIO_ACTUACIONS a
                    LEFT JOIN PIO_PROJECTE p ON a.PROJECTE = p.IDPROJECTE
                    LEFT JOIN PIO_SERVEI_PO s ON a.SERVEIPO = s.IDSERVEI_PO
                """
            df_raw = pd.read_sql(query, ora_conn)

            # Convert Oracle LOBs to Strings
            for col in df_raw.columns:
                if df_raw[col].dtype == 'object':
                    df_raw[col] = df_raw[col].apply(lambda x: x.read() if hasattr(x, 'read') else x)

        print(f"Extracted {len(df_raw)} rows from Oracle (Raw).")
        
    except Exception as e:
        print(f"Oracle Extraction Error: {e}")
        return

    # Filter by valid_cases (JSON)
    if not df_raw.empty:
        initial_count = len(df_raw)
        
        # Normalize CASEID for ensure matching with valid_cases (strip spaces, convert to string)
        df_raw['CASEID_STR'] = df_raw['CASEID'].astype(str).str.strip()
        
        # Filter only the rows where CASEID_STR is in valid_cases
        df_raw = df_raw[df_raw['CASEID_STR'].isin(valid_cases)]
        
        final_count = len(df_raw)
        print(f"   Initial cases: {initial_count}")
        print(f"   Valid cases: {final_count}")
        print(f"   Discards: {initial_count - final_count}")
        
        if df_raw.empty:
            print("No valid cases found after filtering with JSON. Migration will be skipped.")
            return
    else:
        print("No data found in Oracle.")
        return

    # Transform data
    
    df_raw = df_raw.reset_index(drop=True)
    
    df = pd.DataFrame(index=df_raw.index)
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df_raw))]
    
    # Key cleaning
    df_raw['SERVICE_KEY_CLEAN'] = (
        df_raw['SERVICE_KEY']
        .astype(str)
        .str.strip()
        .replace(['None', 'nan', 'NaN', 'NAT', 'None'], None)
    )

    # Mapping service_id using the cleaned SERVICE_KEY
    df['service_id'] = df_raw['SERVICE_KEY_CLEAN'].map(service_map)
    
    # Mapping other fields directly
    df['tracking_code'] = df_raw['CASEID']
    df['name'] = df_raw['TITOL']
    df['alias'] = df_raw['ALIES']
    df['description'] = df_raw['DESCRIPCIOPROJECTE']
    df['address'] = df_raw['ADRECA']
    df['created_at'] = datetime.now()
    df['updated_at'] = datetime.now()

    cols = ['id', 'tracking_code', 'name', 'alias', 'description', 
            'service_id', 'address', 'created_at', 'updated_at']
    df = df[cols]

    # Load Data into MySQL
    print("Loading into MySQL...")
    try:
        df.to_sql('actions', con=mysql_engine, if_exists='append', index=False)
        print(f"Migration successful: {len(df)} actions inserted.")
    except Exception as e:
        print(f"MySQL Loading Error: {e}")



# Migrar TipusActuacio:
# Afegir id de tipus_actuacio i subtipus_actuacio a la taula actions
# Tenim un json que relacion el tipus i subtipus antic amb el nou
# A la taula PIO_ACTUACIONS tenim el camp SUBTIPUSACTUACIO que es un integer que fa referència a la taula PIO_SUBTIPUSACTUACIO
# A la taula PIO_SUBTIPUSACTUACIO tenim el camp IDTIPUSDACTUACIO que fa referència a la taula PIO_TIPUSDACTUACIO
# A la taula PIO_TIPUSACTUACIO tenim el camp TIPUS_ACTUACIO que es el nom del tipus d'actuacio
# A la taula PIO_SUBTIPUSACTUACIO tenim el camp SUBTIPUS_ACTUACIO que es el nom del subtipus d'actuacio
# El json relaciona el nom del tipus i subtipus nou amb el nom del tipus i subtipus antic, per tant haurem de fer un mapping a través d'aquests noms
def update_types(json_file, mysql_url, oracle_config):
    mysql_engine = create_engine(mysql_url)
    
    with open(json_file, "r", encoding="utf-8") as f:
        MAPPING_JSON = json.load(f)

    # Plain mapping dictionaries
    old_to_new_type = {}
    old_to_new_subtype = {}

    for type_key, content in MAPPING_JSON.items():
        # Type
        for old_name in content.get("tipus", []):
            old_to_new_type[old_name] = type_key
        # Subtype
        for subtype_key, old_list in content.get("subtipus", {}).items():
            for old_name in old_list:
                old_to_new_subtype[old_name] = subtype_key

    # Obtain UUID mapping for action types from MySQL
    try:
        at_df = pd.read_sql("SELECT id, `key` FROM action_types", mysql_engine)
        
        key_to_uuid = dict(zip(at_df['key'], at_df['id']))
    except Exception as e:
        print(f"Error reading action_types from MySQL: {e}")
        return

    df = pd.DataFrame()
    try:
        with oracledb.connect(user=oracle_config["user"], 
                             password=oracle_config["password"], 
                             dsn=oracle_config["dsn"]) as ora_conn:
            
            query = """
                SELECT 
                    a.CASEID, 
                    t.TIPUSACTUACIO AS OLD_TYPE,
                    s.SUBTIPUSACTUACIO AS OLD_SUBTYPE
                FROM PIO_ACTUACIONS a
                LEFT JOIN PIO_PROJECTE p ON a.PROJECTE = p.IDPROJECTE
                LEFT JOIN PIO_SUBTIPUSDACTUACIO s ON p.SUBTIPUSACTUACIO = s.IDSUBTIPUSDACTUACIO
                LEFT JOIN PIO_TIPUSDACTUACIO t ON s.IDTIPUSDACTUACIO = t.IDTIPUSDACTUACIO
            """
            df = pd.read_sql(query, ora_conn)
            
        print(f"{len(df)} rows extracted from Oracle for type mapping.")
    except Exception as e:
        print(f"Error extracting from Oracle: {e}")
        return

  
    if not df.empty:
        df['tracking_code'] = pd.to_numeric(df['CASEID'], errors='coerce').fillna(0).astype(int).astype(str)
        
        # Map OLD_TYPE to new type keys, then to UUIDs
        df['type_uuid'] = df['OLD_TYPE'].map(old_to_new_type).map(key_to_uuid)
        
        # Map OLD_SUBTYPE to new subtype keys, then to UUIDs
        df['subtype_uuid'] = df['OLD_SUBTYPE'].map(old_to_new_subtype).map(key_to_uuid)

        # Filter only rows where tracking_code is not null (valid cases)
        update_df = df[['tracking_code', 'type_uuid', 'subtype_uuid']].dropna(subset=['tracking_code'])
        
        print(f"Row ready to update: {len(update_df)}")
        print(f"   - Types found: {update_df['type_uuid'].notnull().sum()}")
        print(f"   - Subtypes found: {update_df['subtype_uuid'].notnull().sum()}")
        
    else:
        print("No data found in Oracle for type mapping.")
        return


    try:
        with mysql_engine.begin() as conn:
            # Temporary table
            conn.execute(text("""
                CREATE TEMPORARY TABLE temp_update_types (
                    tracking_code VARCHAR(255),
                    type_uuid CHAR(36),
                    subtype_uuid CHAR(36),
                    INDEX(tracking_code)
                );
            """))
            
            # Load data into temporary table
            update_df.to_sql('temp_update_types', con=conn, if_exists='append', index=False)
            
            # Update actions using JOIN with temporary table
            result = conn.execute(text("""
                UPDATE actions a
                INNER JOIN temp_update_types t ON a.tracking_code = t.tracking_code
                SET 
                    a.action_type_id = t.type_uuid,
                    a.action_subtype_id = t.subtype_uuid;
            """))
            
            # Drop temporary table
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS temp_update_types;"))
            
            print(f"Done, rows updated: {result.rowcount}")
            
    except Exception as e:
        print(f"Error updating MySQL: {e}")
