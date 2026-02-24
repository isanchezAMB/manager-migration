import pandas as pd
import oracledb
from sqlalchemy import create_engine, text
from datetime import datetime
import uuid


# migrar services:
# La informació prové de la taula PIO_SERVEI_PO
# key = CODI
# initial = CODI (fiquem el mateix que key, però no sé si hauria de ser així)   
# name = NOM


def migrate_services(mysql_url, oracle_config):
    df = pd.DataFrame()
    
    # Extraction from Oracle
    try:
        with oracledb.connect(**oracle_config) as ora_conn:
            query = "SELECT CODI, NOM FROM PIO_SERVEI_PO WHERE CODI IS NOT NULL"
            
            # Use manual cursor to avoid warnings and premature closures
            cursor = ora_conn.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            cursor.close()
            
    except Exception as e:
        print(f"Extraction Error: {e}")
        return

    # Transform Data & Generate UUIDs
    if df.empty:
        print("No services found to migrate.")
        return

    # Generate a unique UUID for each row
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # Map and format columns
    df['key'] = df['CODI'].astype(str)
    df['initial'] = df['CODI'].astype(str)
    df['name'] = df['NOM']
    df['updated_at'] = datetime.now()
    df['deleted_at'] = None

    # Reorder columns
    df = df[['id', 'key', 'initial', 'name', 'updated_at', 'deleted_at']]

    # Loading into MySQL
    try:
        engine = create_engine(mysql_url)
        with engine.begin() as conn:
            # Drop table to recreate structure with the new ID primary key
            conn.execute(text("DROP TABLE IF EXISTS services;"))
            
            conn.execute(text("""
                CREATE TABLE services (
                    `id` CHAR(36) PRIMARY KEY,
                    `key` VARCHAR(255),
                    `initial` VARCHAR(255),
                    `name` VARCHAR(500),
                    `updated_at` TIMESTAMP NULL,
                    `deleted_at` TIMESTAMP NULL
                );
            """))
            
            # Insert the DataFrame
            df.to_sql('services', con=conn, if_exists='append', index=False)
            
        print(f"Migration successful: {len(df)} services inserted.")
    except Exception as e:
        print(f"Loading Error: {e}")