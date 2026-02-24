from helpers.config import ORACLE_CONFIG, MYSQL_URL, VALID_CASES
from migration.actions import migrate_actions
from helpers.init import json_cases_planificacions


if __name__ == "__main__":
    json_cases_planificacions()
    migrate_actions(MYSQL_URL, ORACLE_CONFIG, VALID_CASES)