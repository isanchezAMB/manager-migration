import oracledb
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from helpers.pg_connection import get_pg_connection



# Migrar ATs:
# Primer creare els projects, per cada at un project
# Hem vist que ells no relacionen economic_items amb projects, així que no ho fem nosaltres
# migrar projects ATs:
# aquesta info prové de la nostra base de dades, de la taula dadeseconomiques_at
# name = tipologia_assistencia 
# project_type_id = id de project_types que sigui "Assitència Tècnica", a dia d'avui fiquem tot com a !!!!!external perquè només tenim info dels externs, els interns no tenen import i per tant no aparaeixen a dadeseconomiques
# code = codi_tipologia # !!!!!!hi ha uns quants que no tenen codi_tipologia perque son dos AT8 per un premi o coses semblants.
# desciption = tipologia_assistencia
# accounting_code = codi_tipologia (segons les dades que tenim)
# action_id = referència a la taula actions, la taula antiga té cas que fa referència al tracking_code de la taula actions, per tant haurem de fer un mapping amb el camp cas de dadeseconomiques_at com a tracking_code per trobar el uuid de l'acció

# NO SÉ QUE ÉS
# project_financial_info
# economic_item_id, està buit a les seves dades
# procedure_id, no sé que és la taula aquesta
# organization_id, suposo que es el nom de l'empresa que fa el contracte, però no hi ha cap taula organization, estrany
# redaction_id, referència a la taula redactions, però no sé ben bé que és aquesta taula
# phase, suposo que es la fase del projecte on està, però no ho sé segur

# NO TENIM INFO
# started_at
# target_date
# ended_at

def migrate_projects_AT(mysql_url):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting AT Projects migration...")

    # Load MySQL dependencies
    print("Loading dependencies...")
    try:
        # Map tracking_code to action_id
        actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        action_map = dict(zip(actions_df['tracking_code'], actions_df['id']))

        # Find project type 'Assistència Tècnica' or fallback to 'External'
        pt_query = "SELECT id FROM project_types WHERE name = 'Assistència Tècnica' LIMIT 1"
        pt_df = pd.read_sql(pt_query, mysql_engine)
        
        if not pt_df.empty:
            at_type_id = pt_df.iloc[0]['id']
            print(f"Found project type: 'Assistència Tècnica' ({at_type_id})")
        else:
            print("Project type 'Assistència Tècnica' not found, trying 'External' fallback...")
            pt_df_ext = pd.read_sql("SELECT id FROM project_types WHERE name = 'External' LIMIT 1", mysql_engine)
            if not pt_df_ext.empty:
                at_type_id = pt_df_ext.iloc[0]['id']
                print(f"Using fallback project type: 'External' ({at_type_id})")
            else:
                print("Error: Neither 'Assistència Tècnica' nor 'External' project types were found.")
                return

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
                codi_tipologia 
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
        
        # Filter out orphan projects
        initial_len = len(df)
        df = df.dropna(subset=['action_id'])
        
        if df.empty:
            print("No valid projects found after mapping.")
            return

        # Generate required fields
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Truncate name and description to 191 characters
        df['name'] = df['tipologia_assistencia'].fillna("AT Sin Nombre").astype(str).str.slice(0, 191)
        df['description'] = df['tipologia_assistencia'].fillna("").astype(str).str.slice(0, 191)
        
        df['code'] = df['codi_tipologia']
        df['accounting_code'] = df['codi_tipologia']
        df['project_type_id'] = at_type_id
        
        # Populate additional fields
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        df['phase'] = 0
        
        # Final column selection
        final_cols = ['id', 'name', 'project_type_id', 'code', 'description', 
                      'accounting_code', 'action_id', 'created_at', 'updated_at', 'phase']
        
        df = df[final_cols]
        
    else:
        print("Postgres returned no data.")
        return

    # Insert data into MySQL
    print("Inserting AT Projects into MySQL...")
    try:
        df.to_sql('projects', con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df)} AT projects inserted.")
        
    except Exception as e:
        print(f"Insertion error: {e}")
        if "Duplicate entry" in str(e):
             print("Warning: Projects with this ID or code might already exist.")





# migrar obres:
# primer crearé els projects, per cada obra un project
# després crearé els economic_items, per cada obra un economic_item relacionat amb el project que acabo de crear
# Hem vist que ells no relacionen economic_items amb projects, així que no ho fem nosaltres
# migrar projects obres:
# aquesta info prové de la nostra base de dades, de la taula dadeseconomiques_obra
# name = "Obra"
# project_type_id = id de project_types que sigui "External"
# code = "Obra"
# desciption = "Obra"
# action_id = referència a la taula actions, la taula antiga té cas que fa referència al tracking_code de la taula actions, per tant haurem de fer un mapping amb el camp cas de dadeseconomiques_obra com a tracking_code per trobar el uuid de l'acció
# accounting_code = "Obra" (segons les dades que tenim)



# NO SÉ QUE ÉS
# project_financial_info
# economic_item_id, està buit a les seves dades
# procedure_id, no sé que és la taula aquesta
# organization_id, suposo que es el nom de l'empresa que fa el contracte, però no hi ha cap taula organization, estrany
# redaction_id, referència a la taula redactions, però no sé ben bé que és aquesta taula
# phase, suposo que es la fase del projecte on està, però no ho sé segur, !!! si es NULL es fica a 0


# NO TENIM INFO
# started_at
# target_date
# ended_at
def migrate_projects_obra(mysql_url):
    mysql_engine = create_engine(mysql_url)
    
    print("Starting Works Projects migration (No Duplicates)...")

    # Load dependencies
    print("Loading dependencies...")
    try:
        # Map tracking_code to action_id
        actions_df = pd.read_sql("SELECT id, tracking_code FROM actions", mysql_engine)
        actions_df['tracking_code'] = actions_df['tracking_code'].astype(str).str.strip()
        action_map = dict(zip(actions_df['tracking_code'], actions_df['id']))

        # Find 'External' project type
        pt_query = "SELECT id FROM project_types WHERE name = 'External' LIMIT 1"
        pt_df = pd.read_sql(pt_query, mysql_engine)
        
        if not pt_df.empty:
            external_type_id = pt_df.iloc[0]['id']
            print(f"Found project type: 'External' ({external_type_id})")
        else:
            print("Error: Project type 'External' not found.")
            return

    except Exception as e:
        print(f"MySQL dependencies error: {e}")
        return

    # Read data from Postgres
    print("Reading from Postgres table dadeseconomiques_obra...")
    df = pd.DataFrame()
    pg_conn = None
    try:
        # Assuming get_pg_connection is defined elsewhere in your file
        pg_conn = get_pg_connection() 
        query = "SELECT cas FROM dadeseconomiques_obra"
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
        
        # Clean tracking code and map action_id
        df['clean_code'] = pd.to_numeric(df['cas'], errors='coerce').fillna(0).astype(int).astype(str)
        df['action_id'] = df['clean_code'].map(action_map)
        
        # Filter out orphan projects
        df = df.dropna(subset=['action_id'])

        if df.empty:
            print("No valid projects found after mapping.")
            return

        # Deduplicate by action_id, keeping the last occurrence
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['action_id'], keep='last')
        after_dedup = len(df)
        
        if before_dedup > after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicate records.")

        # Generate required fields
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Fixed values for 'Obra'
        df['name'] = "Obra"
        df['description'] = "Obra"
        df['code'] = "Obra"            
        df['accounting_code'] = "Obra"
        df['project_type_id'] = external_type_id
        
        # Populate additional fields
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        df['phase'] = 0

        # Final column selection
        final_cols = ['id', 'name', 'project_type_id', 'code', 'description', 
                      'accounting_code', 'action_id', 'created_at', 'updated_at', 'phase']
        
        df = df[final_cols]
        
    else:
        print("Postgres returned no data.")
        return

    # Insert data into MySQL
    print("Inserting Works Projects into MySQL...")
    try:
        df.to_sql('projects', con=mysql_engine, if_exists='append', index=False)
        print(f"Success! {len(df)} projects inserted.")
        
    except Exception as e:
        print(f"Insertion error: {e}")
        if "Duplicate entry" in str(e):
             print("Warning: Duplicate entry error (likely 'code'='Obra' violates a UNIQUE constraint).")