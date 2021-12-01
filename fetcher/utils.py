from functools import lru_cache, wraps

from motor import MotorDatabase

from common.config import get_config
from common.database import init_cached_database, parse_mongodb_connection_string


@lru_cache
def init_database(async_flag=True) -> MotorDatabase:
    config = get_config('omics-database')
    db = init_cached_database(parse_mongodb_connection_string(
        **config), db_name=config['db_name'], async_flag=async_flag)
    return db
