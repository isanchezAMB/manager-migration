import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.mysql import LONGTEXT
from helpers.config import DB_URL_BASE


def clone_database(source, destination):
    engine_source = create_engine(f"{DB_URL_BASE}/{source}")
    engine_destination = create_engine(f"{DB_URL_BASE}/{destination}")
    
    # Get list of tables
    inspector = inspect(engine_source)
    tables = inspector.get_table_names()
    
    print(f"Starting cloning from '{source}' to '{destination}'...")

    for table in tables:
        print(f"Copying table: {table}...{' ' * 20}", end="\r")
        
        # Read data using backticks to avoid errors with reserved words
        query = f"SELECT * FROM `{table}`" 
        df = pd.read_sql(query, engine_source)
        
        # Type mapping to prevent "Data too long" errors
        dtype_map = {}
        critical_columns = [
            'geometry', 'phase_config', 'phase_0_questionary', 
            'phase_1_questionary', 'phase_2_questionary', 
            'phase_3_questionary', 'final_questionary', 
            'programme_membership_data', 'uploaded_documents'
        ]
        
        for col in critical_columns:
            if col in df.columns:
                dtype_map[col] = LONGTEXT

        # Write data to destination
        # pandas handles backticks automatically when creating/inserting
        df.to_sql(
            table, 
            engine_destination, 
            if_exists='replace', 
            index=False, 
            dtype=dtype_map
        )
        
    print(f"\nSuccess! Cloned {len(tables)} tables successfully.")

if __name__ == "__main__":
    clone_database('defaultdb', 'testdb')