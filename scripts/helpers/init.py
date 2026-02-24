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