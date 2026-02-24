from sqlalchemy import create_engine, text
from helpers.pg_connection import get_pg_connection
import pandas as pd
import json



# Function to export only cases with planification data to a JSON file
def json_cases_planificacions():
    
    try:
        conn = get_pg_connection()
        query = "SELECT DISTINCT cas FROM dadesplanificacions"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return

    if df.empty:
        print("Empty table")
        return

    # Clean
    lista_casos = df['cas'].astype(int).astype(str).str.strip().unique().tolist()
    
    # Remove empty strings if any
    lista_casos = [c for c in lista_casos if c != '']
    
    # save to JSON
    nombre_archivo = "data/casos_planificacions.json"
    
    with open(nombre_archivo, 'w') as f:
        json.dump(lista_casos, f)
        
    print(f"Done, json saved in '{nombre_archivo}'.")
    print(f"Number of cases: {len(lista_casos)}")


def clean_database(mysql_url):
    
    # Safety check
    confirm = input("Are you sure you want to clean the database? Type 'Y' to confirm: ")
    if confirm != "Y":
        print("Aborting database cleaning.")
        return

    engine = create_engine(mysql_url)

    # List of tables to clean
    tables_to_clean = [
        "economic_item_anual_budgets", 
        "economic_items",              
        "certifications",              
        "projects",                    
        "actions",                     
        "services",                     
        "action_city",
        "programs",
        "action_program"
    ]

    try:
        with engine.connect() as conn:
            # Deactivate foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

            # Clean each table
            for table in tables_to_clean:
                print(f"   Cleaning: {table}...")
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table};"))
                except Exception as e:
                    print(f"      Error truncating table {table}: {e}")

            # Reactivate foreign key checks
            print("   Reactivating foreign key checks...")
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            
            # Commit changes
            conn.commit()
            
        print("Database cleaned successfully.")

    except Exception as e:
        print(f"Fatal error during cleaning: {e}")