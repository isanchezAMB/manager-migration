from helpers.config import ORACLE_CONFIG, MYSQL_URL, VALID_CASES
from migration.actions import migrate_actions, update_types, migrate_action_city
from helpers.init import json_cases_planificacions, clean_database
from migration.services import migrate_services
from migration.programs import migrate_programs
from migration.fundings import migrate_fundings
from migration.projects import migrate_projects_AT, migrate_projects_obra
from migration.certifications import migrate_certifications_AT, migrate_certifications_obra


if __name__ == "__main__":
    clean_database(MYSQL_URL)
    json_cases_planificacions()

    migrate_services(MYSQL_URL, ORACLE_CONFIG)

    migrate_actions(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)
    update_types("data/map_type_subtype.json", MYSQL_URL, ORACLE_CONFIG)
    migrate_action_city(MYSQL_URL, ORACLE_CONFIG)

    migrate_programs(MYSQL_URL, ORACLE_CONFIG)
    migrate_fundings(MYSQL_URL, ORACLE_CONFIG)

    migrate_projects_AT(MYSQL_URL)
    migrate_projects_obra(MYSQL_URL)

    migrate_certifications_AT(MYSQL_URL, ORACLE_CONFIG, "data/map_assistance_type.json")
    migrate_certifications_obra(MYSQL_URL, ORACLE_CONFIG)
