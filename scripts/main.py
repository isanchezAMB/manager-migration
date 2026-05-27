from helpers.config import ORACLE_CONFIG, MYSQL_URL, DB_URL_BASE, VALID_CASES
from helpers.init import json_cases_planificacions, clean_database, copy_reference_tables
from migration.users import migrate_users
from migration.services import migrate_services
from migration.actions import migrate_actions, migrate_action_city
from migration.programs import migrate_programs
from migration.fundings import migrate_fundings
from migration.projects import migrate_projects_AT, migrate_projects_obra
from migration.certifications import migrate_certifications_AT, migrate_certifications_obra
from migration.economic import (
    migrate_economic_items_AT, migrate_economic_items_obra,
    migrate_economic_items_anual_budgets_AT, migrate_economic_items_anual_budgets_obra,
)

# URL de la BD de referencia donde vienen las tablas precargadas por la empresa externa
REFERENCE_DB_URL = f"{DB_URL_BASE}/testdb"


if __name__ == "__main__":

    # ------------------------------------------------------------------
    # 0. Preparación
    # ------------------------------------------------------------------
    # Limpia todas las tablas migradas (pide confirmación)
    clean_database(MYSQL_URL)

    # Regenera el JSON de casos válidos desde PostgreSQL
    json_cases_planificacions()

    # Copia tablas de referencia precargadas (action_types, cities)
    print("Copying reference tables...")
    copy_reference_tables(REFERENCE_DB_URL, MYSQL_URL)

    # ------------------------------------------------------------------
    # 1. Usuarios y servicios (base para el resto de FKs)
    # ------------------------------------------------------------------
    migrate_users(MYSQL_URL, ORACLE_CONFIG)
    migrate_services(MYSQL_URL, ORACLE_CONFIG)

    # ------------------------------------------------------------------
    # 2. Acciones (núcleo del modelo)
    # ------------------------------------------------------------------
    migrate_actions(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)
    migrate_action_city(MYSQL_URL, ORACLE_CONFIG)

    # ------------------------------------------------------------------
    # 3. Programas y financiaciones
    # ------------------------------------------------------------------
    migrate_programs(MYSQL_URL, ORACLE_CONFIG)
    migrate_fundings(MYSQL_URL, ORACLE_CONFIG)

    # ------------------------------------------------------------------
    # 4. Proyectos
    # ------------------------------------------------------------------
    migrate_projects_AT(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)
    migrate_projects_obra(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)

    # ------------------------------------------------------------------
    # 5. Certificaciones
    # ------------------------------------------------------------------
    migrate_certifications_AT(MYSQL_URL, ORACLE_CONFIG, "data/map_assistance_type.json")
    migrate_certifications_obra(MYSQL_URL, ORACLE_CONFIG)

    # ------------------------------------------------------------------
    # 6. Datos económicos
    # ------------------------------------------------------------------
    migrate_economic_items_AT(MYSQL_URL)
    migrate_economic_items_obra(MYSQL_URL)
    migrate_economic_items_anual_budgets_AT(MYSQL_URL)
    migrate_economic_items_anual_budgets_obra(MYSQL_URL, ORACLE_CONFIG)
