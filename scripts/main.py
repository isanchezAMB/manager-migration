from helpers.config import ORACLE_CONFIG, MYSQL_URL, VALID_CASES
from migration.actions import migrate_actions, update_types, migrate_action_city
from helpers.init import json_cases_planificacions, clean_database
from migration.services import migrate_services
from migration.programs import migrate_programs


if __name__ == "__main__":
    clean_database(MYSQL_URL)
    json_cases_planificacions()

    migrate_services(MYSQL_URL, ORACLE_CONFIG)

    migrate_actions(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)
    update_types("data/map_type_subtype.json", MYSQL_URL, ORACLE_CONFIG)
    migrate_action_city(MYSQL_URL, ORACLE_CONFIG)

    migrate_programs(MYSQL_URL, ORACLE_CONFIG)