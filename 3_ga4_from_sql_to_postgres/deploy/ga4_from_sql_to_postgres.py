import traceback
from pathlib import Path
import glob
import configparser
import psycopg2


class MyException(Exception):
    pass


def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def main():
    config_path = Path.cwd() / "ga4_from_sql_to_postgres.ini"
    config = read_config(config_path)
    local_in_home = Path.cwd() / config["local"]["sql_home"]

    ddl_list = glob.glob(str(local_in_home) + "/events_*_ddl.sql")
    print(f"DDLファイル数：{len(ddl_list)}")
    if len(ddl_list) != 1:
        raise MyException(f"DDLファイルは1つでなければなりません。処理を中断します。")

    sql_list = glob.glob(str(local_in_home) + "/*/events_*.sql")
    print(f"INSERTファイル数：{len(sql_list)}")
    sql_list.sort(reverse=True)

    host = config["postgresql"]["host"]
    port = config["postgresql"]["port"]
    dbname = config["postgresql"]["dbname"]
    user = config["postgresql"]["user"]
    password = config["postgresql"]["password"]

    with psycopg2.connect(f"host={host} port={port} dbname={dbname} user={user} password={password}") as conn:
        with conn.cursor() as cur:
            print(f"DDLを実行します")
            with open(ddl_list[0], "rt", encoding="utf-8") as fi:
                sql = fi.read()
                cur.execute(sql)
            print("  ... done.")

            print(f"INSERT文を実行します")
            for index, in_path_str in enumerate(sql_list, 1):
                in_path = Path(in_path_str)
                print(f"({index}) パーティション: {in_path.stem}:\n    {in_path_str} ... ", end="")
                with open(in_path, "rt", encoding="utf-8") as fi:
                    sql = fi.read()
                    cur.execute(sql)
                print("done.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
