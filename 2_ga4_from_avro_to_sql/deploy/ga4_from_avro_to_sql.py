import datetime
import traceback
import json
import datetime
from pathlib import Path
import glob
import configparser
import textwrap
import fastavro
from collections import deque
from psycopg2.extensions import adapt

COMMIT_NUM = 100
DT_UTC_AWARE = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


class MyException(Exception):
    pass


class MyReader(fastavro.reader):
    def __init__(self, file):
        super().__init__(file)
        self.meta = self._header["meta"]


def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def escape(ss):
    return str(adapt(ss.encode("utf-8").decode("latin-1")))


def convert_to_postgres_type(avro_type, default_value, ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type):
    postgres_type = None
    default_str = None
    nullable = False

    if default_value is None:
        default_str = "NULL"

    if type(avro_type) is list:
        if "null" in avro_type:
            nullable = True
            avro_type.remove("null")
        assert len(avro_type) == 1
        avro_type = avro_type[0]

    if type(avro_type) is str:
        if avro_type == "string":
            postgres_type = "VARCHAR"
            if default_value is not None:
                default_str = escape(default_value)
        elif avro_type == "bytes":
            postgres_type = "BYTEA"
            if default_value is not None:
                pass
        elif avro_type == "long":
            postgres_type = "BIGINT"
            if default_value is not None:
                default_str = str(default_value)
        elif avro_type == "double":
            postgres_type = "DOUBLE PRECISION"
            if default_value is not None:
                default_str = str(default_value)
        elif avro_type == "boolean":
            postgres_type = "BOOLEAN"
            if default_value is not None:
                if default_value:
                    default_str = "TRUE"
                else:
                    default_str = "FALSE"
    elif type(avro_type) is dict:
        if "sqlType" in avro_type:
            sql_type = avro_type["sqlType"]
            if sql_type == "JSON":
                postgres_type = "JSON"
                if default_value is not None:
                    default_str = escape(default_value)
        elif "logicalType" in avro_type:
            logical_type = avro_type["logicalType"]
            if logical_type == "decimal":
                postgres_type = "NUMERIC"
                if default_value is not None:
                    default_str = str(default_value)
            elif logical_type in ("timestamp-millis", "timestamp-micros"):
                postgres_type = "TIMESTAMP WITH TIME ZONE"
                if default_value is not None:
                    default_str = escape(default_value.isoformat())
            elif logical_type == "date":
                postgres_type = "DATE"
                if default_value is not None:
                    default_str = escape(default_value.isoformat())
            elif logical_type in ("time-millis", "time-micros"):
                postgres_type = "TIME WITH TIME ZONE"
                if default_value is not None:
                    default_str = escape(default_value.isoformat())
            elif logical_type in ("local-timestamp-millis", "local-timestamp-micros"):
                postgres_type = "TIMESTAMP"
                if default_value is not None:
                    default_str = escape(default_value.isoformat())
        else:
            avro_child_type = avro_type["type"]
            if avro_child_type == "array":
                default_value = None
                postgres_items_type, _, _ = convert_to_postgres_type(
                    avro_type["items"], default_value, ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type)
                if type(postgres_items_type) is str:
                    postgres_type = {
                        "array": postgres_items_type + "[]",
                        "items": postgres_items_type
                    }
                else:
                    postgres_type = {
                        "array": postgres_items_type["record"] + "[]",
                        "items": postgres_items_type
                    }
            elif avro_child_type == "record":
                len_is_serial_of_postgres_record_type.append(True)
                postgres_record_type = f"{postgres_record_type_prefix}_type_{len(len_is_serial_of_postgres_record_type)}"
                postgres_fields_type = make_sql_create_type(
                    postgres_record_type, avro_type["fields"], ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type)
                postgres_type = {
                    "record": postgres_record_type,
                    "fields": postgres_fields_type
                }
    return postgres_type, default_str, nullable


def make_sql_create_type(name, avro_fields, ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type):
    ddl_stmt = ""
    postgres_fields_type = []
    for field in avro_fields:
        default_value = None
        postgres_type, _, _ = convert_to_postgres_type(
            field["type"], default_value, ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type)
        postgres_fields_type.append(postgres_type)
        if len(ddl_stmt) == 0:
            ddl_stmt = f"CREATE TYPE {name} AS (\n    "
        else:
            ddl_stmt += "\n  , "
        ddl_stmt += field["name"] + ' '
        if type(postgres_type) is str:
            ddl_stmt += postgres_type
        elif "record" in postgres_type:
            ddl_stmt += postgres_type["record"]
        else:
            ddl_stmt += postgres_type["array"]
    if 0 < len(ddl_stmt):
        ddl_stmt += "\n);\n"
        ddl_queue.append(ddl_stmt)
    return postgres_fields_type


def make_sql_create_table(name, avro_fields, ddl_queue, postgres_record_type_prefix):
    ddl_stmt = ""
    postgres_type_list = []
    len_is_serial_of_postgres_record_type = []
    for field in avro_fields:
        if "default" in field:
            default_value = field["default"]
        else:
            default_value = None
        postgres_type, default_str, nullable = convert_to_postgres_type(
            field["type"], default_value, ddl_queue, postgres_record_type_prefix, len_is_serial_of_postgres_record_type)
        postgres_type_list.append(postgres_type)
        if len(ddl_stmt) == 0:
            ddl_stmt = f"    "
        else:
            ddl_stmt += "\n  , "
        ddl_stmt += field["name"] + ' '
        if type(postgres_type) is str:
            ddl_stmt += postgres_type
        elif "record" in postgres_type:
            ddl_stmt += postgres_type["record"]
        else:
            ddl_stmt += postgres_type["array"]
        if not nullable:
            ddl_stmt += " NOT NULL"
        if "default" in field:
            ddl_stmt += f" DEFAULT {default_str}"

        if field["name"] == "event_timestamp":
            ddl_stmt += f"\n  , eventtimestamp TIMESTAMP WITH TIME ZONE DEFAULT NULL"

    if 0 < len(ddl_stmt):
        ddl_stmt += "\n)"
        ddl_queue.append(ddl_stmt)
    return postgres_type_list


def convert_to_postgres_value(avro_type, postgres_type, avro_value, null_if_convert_error):
    if avro_value is None:
        return "NULL"

    try:
        value_str = ""

        if type(avro_type) is list:
            if "null" in avro_type:
                avro_type.remove("null")
            assert len(avro_type) == 1
            avro_type = avro_type[0]

        if type(avro_type) is str:
            if avro_type == "string":
                value_str = escape(avro_value)
            elif avro_type == "long":
                value_str = str(avro_value)
            elif avro_type == "double":
                value_str = str(avro_value)
            elif avro_type == "boolean":
                if avro_value:
                    value_str = "TRUE"
                else:
                    value_str = "FALSE"
        elif type(avro_type) is dict:
            if "sqlType" in avro_type:
                sql_type = avro_type["sqlType"]
                if sql_type == "JSON":
                    value_str = escape(avro_value)
            elif "logicalType" in avro_type:
                logical_type = avro_type["logicalType"]
                if logical_type == "decimal":
                    value_str = str(avro_value)
                elif logical_type in ("timestamp-millis", "timestamp-micros"):
                    value_str = escape(avro_value.isoformat())
                elif logical_type == "date":
                    value_str = escape(avro_value.isoformat())
                elif logical_type in ("time-millis", "time-micros"):
                    value_str = escape(avro_value.isoformat())
                elif logical_type in ("local-timestamp-millis", "local-timestamp-micros"):
                    value_str = escape(avro_value.isoformat())
            else:
                avro_child_type = avro_type["type"]
                if avro_child_type == "array":
                    avro_items_type = avro_type["items"]
                    postgres_items_type = postgres_type["items"]
                    postgres_value_list = []
                    for val in avro_value:
                        postgres_value_list.append(convert_to_postgres_value(
                            avro_items_type, postgres_items_type, val, null_if_convert_error))
                    value_str = f'ARRAY[{",".join(postgres_value_list)}]::{postgres_type["array"]}'
                elif avro_child_type == "record":
                    avro_fields = avro_type["fields"]
                    postgres_fields_type = postgres_type["fields"]
                    postgres_value_list = []
                    for avro_field, postgres_field_type in zip(avro_fields, postgres_fields_type):
                        postgres_value_list.append(convert_to_postgres_value(
                            avro_field["type"], postgres_field_type, avro_value[avro_field["name"]], null_if_convert_error))
                    value_str = f'ROW({",".join(postgres_value_list)})'

        if len(value_str) == 0:
            raise MyException(f"Convert Error: type={avro_type}, value={avro_value}")
    except Exception as e:
        if null_if_convert_error:
            return "NULL"
        else:
            raise e
    else:
        return value_str


def make_sql_insert(tablename, schema, postgres_type_list, rec, null_if_convert_error=False):
    insert_into = []
    insert_values = []
    fields = schema["fields"]

    for field, postgres_type in zip(fields, postgres_type_list):
        field_name = field["name"]
        insert_into.append(field_name)
        insert_values.append(convert_to_postgres_value(field["type"], postgres_type, rec[field_name], null_if_convert_error))

        if field_name == "event_timestamp":
            insert_into.append("eventtimestamp")
            dt = DT_UTC_AWARE + datetime.timedelta(microseconds=rec[field_name])
            insert_values.append(escape(dt.isoformat()))

    insert_into_str = "\n  , ".join(insert_into)
    insert_values_str = "\n  , ".join(insert_values)
    insert_stmt = f"INSERT INTO {tablename} (\n    {insert_into_str}\n)\nVALUES (\n    {insert_values_str}\n);\n"
    return insert_stmt


def main():
    config_path = Path.cwd() / "ga4_from_avro_to_sql.ini"
    config = read_config(config_path)
    local_in_home = Path.cwd() / config["local"]["avro_home"]
    local_out_home = Path.cwd() / config["local"]["sql_home"]

    avro_list = glob.glob(str(local_in_home) + "/*/events_*.avro")
    print(f"Avroファイル数：{len(avro_list)}")
    if len(avro_list) == 0:
        return

    avro_list.sort(reverse=True)
    postgres_record_type_prefix = "events"

    # DDL
    partition_stmt_list = []
    local_out_home.mkdir(parents=True, exist_ok=True)  # ディレクトリがなければ作成
    out_ddl_path = local_out_home / f"{Path(avro_list[0]).stem.lower()}_ddl.sql"
    with open(out_ddl_path, "wt", encoding="utf-8") as fo_ddl:
        try:
            for index, in_path_str in enumerate(avro_list, 1):
                in_path = Path(in_path_str)
                table_name = in_path.stem.lower()

                with open(in_path, "rb") as fi:
                    reader = MyReader(fi)
                    schema = json.loads(reader.meta["avro.schema"].decode("utf-8"))

                    ddl_queue = deque()
                    make_sql_create_table(table_name, schema["fields"], ddl_queue, postgres_record_type_prefix)
                    create_table_stmt = ddl_queue.pop()
                    create_type_stmt = "".join(ddl_queue)

                    date_from = datetime.datetime.strptime(table_name[7:], "%Y%m%d")
                    date_to = date_from + datetime.timedelta(days=1)
                    partition_stmt = f"""
                        CREATE TABLE {table_name}
                            PARTITION OF events
                            FOR VALUES FROM ('{date_from.strftime("%Y%m%d")}') TO ('{date_to.strftime("%Y%m%d")}');\n
                    """
                    partition_stmt_list.append(textwrap.dedent(partition_stmt)[1:-1])
                    if index == 1:
                        create_table_stmt0 = create_table_stmt
                        create_type_stmt0 = create_type_stmt
                    elif create_table_stmt == create_table_stmt0 and create_type_stmt == create_type_stmt0:
                        pass
                    else:
                        raise MyException(f"Avroファイル間にスキーマの差異が検出されました。処理を中断します。{in_path_str}")
        finally:
            # トランザクション開始
            fo_ddl.write("BEGIN;\n")

            # DDL
            fo_ddl.write(create_type_stmt)
            fo_ddl.write(f"CREATE TABLE events (\n{create_table_stmt} PARTITION BY RANGE (event_date);\n")
            for partition_stmt in reversed(partition_stmt_list):
                fo_ddl.write(partition_stmt)

            # トランザクション終了
            fo_ddl.write("COMMIT;\n")

    local_in_home_str = str(local_in_home.absolute())
    local_out_home_str = str(local_out_home.absolute())

    # INSERT文
    for index, in_path_str in enumerate(avro_list, 1):
        in_path = Path(in_path_str)
        table_name = in_path.stem.lower()

        in_path_parent_str = str(in_path.parent.absolute())
        out_dir = Path(in_path_parent_str.replace(local_in_home_str, local_out_home_str))

        out_dir.mkdir(parents=True, exist_ok=True)  # ディレクトリがなければ作成
        out_path = out_dir / f"{table_name}.sql"
        with open(in_path, "rb") as fi, open(out_path, "wt", encoding="utf-8") as fo:
            reader = MyReader(fi)
            schema = json.loads(reader.meta["avro.schema"].decode("utf-8"))

            ddl_queue = deque()
            postgres_type_list = make_sql_create_table(table_name, schema["fields"], ddl_queue,
                                                       postgres_record_type_prefix)

            # トランザクション開始
            fo.write("BEGIN;\n")

            # INSERT文
            num = 0
            for rec in reader:
                num += 1
                insert_stmt = make_sql_insert(table_name, schema, postgres_type_list, rec, null_if_convert_error=False)
                fo.write(insert_stmt)
                if num % COMMIT_NUM == 0:
                    fo.write("COMMIT;\nBEGIN;\n")
            print(f"({index}) {table_name}.sql: {num} 行")

            # トランザクション終了
            fo.write("COMMIT;\n")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
