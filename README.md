# Manager Migration

Data migration scripts to transfer action management data from legacy Oracle and PostgreSQL systems into a new MySQL/MariaDB database. Developed as part of a migration project for the Àrea Metropolitana de Barcelona (AMB).

## Overview

The migration moves the following entities from the legacy systems into the target database, in dependency order:

| Step | Entity | Source |
|------|--------|--------|
| 0 | Reference tables (action types, cities) | MySQL reference DB |
| 1 | Users | Oracle (PIO_*) |
| 1 | Services | Oracle (PIO_*) |
| 2 | Actions + city relations | PostgreSQL CSV export |
| 3 | Programs | Oracle / CSV fallback |
| 3 | Fundings | Oracle / CSV fallback |
| 4 | Projects (AT and Obra) | Oracle / CSV fallback |
| 5 | Certifications (AT and Obra) | Oracle / CSV fallback |
| 6 | Economic items and annual budgets | Oracle / CSV fallback |

## Project Structure

```
scripts/
├── main.py                  # Entry point — runs the full migration in order
├── helpers/
│   ├── config.py            # Loads credentials from environment variables
│   ├── init.py              # DB cleanup, reference table copy, JSON generation
│   ├── pg_connection.py     # PostgreSQL connection helper
│   └── clone_cb.py          # Database cloning utility
└── migration/
    ├── users.py
    ├── services.py
    ├── actions.py
    ├── programs.py
    ├── fundings.py
    ├── projects.py
    ├── certifications.py
    └── economic.py
data/
├── casos_planificacions.json      # Set of valid case IDs to migrate
├── map_assistance_type.json       # Old task names → assistance type codes
├── map_type_subtype.json          # Old type/subtype names → new keys
├── pg_export/                     # PostgreSQL CSV exports (not included)
└── oracle_export/                 # Oracle CSV exports (not included)
20260506_ambgpo_test_database_schema.sql   # Target database schema
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```env
MYSQL_URL=mysql+pymysql://user:password@host:port/database
DB_URL_BASE=mysql+pymysql://user:password@host:port

ORACLE_HOST=your_oracle_host
ORACLE_PORT=1521
ORACLE_SERVICE=your_service_name
ORACLE_USER=your_user
ORACLE_PASSWORD=your_password

PG_HOST=your_pg_host
PG_DATABASE=your_database
PG_USER=your_user
PG_PASSWORD=your_password
```

### 3. Data sources

The script connects directly to Oracle and PostgreSQL. If you do not have access to these databases, place the CSV exports in the following directories instead and the script will use them as fallback:

- `data/pg_export/` — PostgreSQL exports
- `data/oracle_export/` — Oracle exports

### 4. Apply the schema

Run the SQL schema file against the target MySQL database before executing the migration:

```bash
mysql -u user -p database < 20260506_ambgpo_test_database_schema.sql
```

## Running the Migration

```bash
cd scripts
python main.py
```

The script will:
1. Ask for confirmation before cleaning the target database
2. Regenerate the valid cases list from PostgreSQL
3. Copy reference tables from the reference database
4. Run all migration steps in order

Each step is idempotent — running it again will insert new records and update existing ones without creating duplicates.

## Dependencies

- `oracledb` — Oracle database driver
- `psycopg2` — PostgreSQL driver
- `SQLAlchemy` — SQL toolkit and ORM
- `PyMySQL` — MySQL driver
- `pandas` — Data manipulation
- `python-dotenv` — Environment variable loading
