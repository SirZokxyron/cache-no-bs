import sqlite3
import tomllib
import pendulum
import hashlib

DEFAULT_CONFIG = """
[config]
cache_directory = 'cache/'             # make sure it exists :)
database_filename = 'cnb'
max_cached_files = 10                  # value in number of files
cached_files_lifetime = 3600           # value in seconds
cache_type = 'FIFO'                    # options are [ 'FIFO', 'LRU' ]
"""

try:
    with open("cnb_config.toml", "r") as fp:
        ENV = tomllib.loads(fp.read())
except FileNotFoundError:
    with open("cnb_config.toml", "w") as fp:
        fp.write(DEFAULT_CONFIG)
    ENV = tomllib.loads(DEFAULT_CONFIG)


def compute_hash_filename(key: str, ext: str = "") -> str:
    hash = hashlib.md5(key.encode())
    digest = hash.hexdigest()
    return digest + ext


def create_cache_database() -> None:
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS cache (
        name TEXT NOT NULL PRIMARY KEY,
        cache_date DATE
    )
    """
    cursor.execute(query)
    conn.commit()


def clear_database():
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = "DELETE FROM cache"
    cursor.execute(query)
    conn.commit()


def get_database_entries():
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = "SELECT * FROM cache"
    cursor.execute(query)
    entries = cursor.fetchall()
    conn.commit()
    return entries


def get_number_of_cached_files() -> int:
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM cache"
    cursor.execute(query)
    number = cursor.fetchone()[0]
    return number


def save_to_cache(key, data, date: str = pendulum.now().to_datetime_string()):
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()

    query = "SELECT COUNT(*) FROM cache"
    cursor.execute(query)
    number = cursor.fetchone()[0]
    if number == ENV["config"]["max_cached_files"]:
        delete_oldest_entry()

    filename = compute_hash_filename(key)
    with open(f"{ENV['config']['cache_directory']}{filename}", "w") as fp:
        fp.write(data)

    query = f"INSERT INTO cache VALUES('{filename}', '{date}')"
    cursor.execute(query)
    conn.commit()
    pass


def retrieve_from_cache_if_exists(key: str) -> tuple[bool, tuple | None]:
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    filename = compute_hash_filename(key)
    query = f"SELECT * FROM cache WHERE name = '{filename}';"
    cursor.execute(query)
    result = cursor.fetchone()
    if result and ENV["config"]["cache_type"] == "LRU":
        update_entry_date(key, pendulum.now().to_datetime_string())
    return (result is not None, result)


def delete_oldest_entry():
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = "DELETE FROM cache WHERE name IN (SELECT name FROM cache ORDER BY cache_date LIMIT 1)"
    cursor.execute(query)
    conn.commit()


def delete_expired_entries():
    delta = ENV["config"]["cached_files_lifetime"]
    if delta == -1:
        return
    threshold_date = pendulum.now().subtract(seconds=delta).to_datetime_string()
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    query = f"DELETE FROM cache WHERE name IN (SELECT name FROM cache WHERE cache_date < '{threshold_date}')"
    cursor.execute(query)
    conn.commit()


def update_entry_date(key: str, new_date: str):
    conn = sqlite3.connect(f"{ENV['config']['database_filename']}.sqlite")
    cursor = conn.cursor()
    filename = compute_hash_filename(key)
    query = f"""
    UPDATE cache
    SET cache_date = '{new_date}'
    WHERE name = '{filename}'
    """
    cursor.execute(query)
    conn.commit()


def get_data_from_cache(key: str) -> str:
    filename = compute_hash_filename(key)
    with open(f"{ENV['config']['cache_directory']}{filename}", "r") as fp:
        data = fp.read()
    return data
