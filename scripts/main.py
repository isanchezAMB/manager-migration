from helpers.config import ORACLE_CONFIG, MYSQL_URL, VALID_CASES
from migration.actions import migrate_actions, update_types
from helpers.init import json_cases_planificacions, clean_database


if __name__ == "__main__":
    clean_database(MYSQL_URL)
    json_cases_planificacions()


    migrate_actions(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)
    update_types("data/map_type_subtype.json", MYSQL_URL, ORACLE_CONFIG)