import traceback
from pathlib import Path
import configparser
from google.cloud import storage, bigquery


def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def gs_path_exists(gs_bucket, gs_path):
    blob = gs_bucket.blob(gs_path)
    return blob.exists()


def from_bq_to_gs(dataset_ref, table_name, bq_client, gs_path):
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.ExtractJobConfig(compression="DEFLATE", destination_format="AVRO")
    job = bq_client.extract_table(table_ref, gs_path, job_config=job_config)
    job.result()


def from_gs_to_local(gs_bucket, gs_path, local_path):
    blob = gs_bucket.blob(gs_path)
    blob.download_to_filename(local_path)


def main():
    config_path = Path.cwd() / "ga4_from_bq_to_avro.ini"
    config = read_config(config_path)

    bq_client = bigquery.Client()
    gs_client = storage.Client()

    bq_project = config["BigQuery"]["project"]
    bq_dataset = config["BigQuery"]["dataset"]
    gs_bucket = gs_client.get_bucket(config["GCS"]["bucket"])
    gs_home = bq_dataset
    local_home = Path.cwd() / bq_dataset

    dataset_ref = bigquery.DatasetReference(bq_project, bq_dataset)
    tables = bq_client.list_tables(dataset_ref)
    table_list = [table.table_id for table in tables if table.table_id[:7] == "events_"]
    table_list.sort(reverse=True)
    print(f"eventsテーブル数：{len(table_list)}")

    for table_name in table_list:
        yyyymm = table_name[7:13]
        local_dir = local_home / yyyymm
        file_name = f"{table_name}.avro"
        local_path = local_dir / file_name

        # localファイル存在チェック
        if local_path.exists():
            print(f"skip:\n    local exists: {local_path}")
            continue

        gs_path = f"{gs_home}/{yyyymm}/{file_name}"
        gs_fullpath = f"gs://{gs_bucket.name}/{gs_path}"

        # GCSファイル存在チェック
        if gs_path_exists(gs_bucket, gs_path):
            print(f"skip BigQuery:    GCS exists: {gs_path}")
        else:
            # BigQuery -> GCS
            from_bq_to_gs(dataset_ref, table_name, bq_client, gs_fullpath)

        # GCS -> local
        print(f"download:\n    {gs_fullpath}\n    -> {local_path}")
        local_dir.mkdir(parents=True, exist_ok=True)    # ディレクトリがなければ作成
        from_gs_to_local(gs_bucket, gs_path, local_path)
        print("  ... done.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
