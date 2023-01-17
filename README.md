GA4のテーブルをBigQueryからPostgreSQLにAvroファイル経由でコピーする<br>
https://qiita.com/kanedaq/items/c3b471b6d0d73c46392b


# 実行手順

## (1) ga4_from_bq_to_avro

### build

```sh
cd 1_ga4_from_bq_to_avro
docker build ./ -t ga4_from_bq_to_avro
```

### run

- Google Cloudのサービスアカウントファイルを service_account.json の名前で実行ディレクトリにコピーする。<br>
  サービスアカウントファイルについて：https://cloud.google.com/docs/authentication/production?hl=ja
- ga4_from_bq_to_avro.ini を実行ディレクトリにコピーして編集する。
- 以下を参考にして docker run を実行する。

```sh
docker run --mount type=bind,source="$(pwd)",target=/workdir ga4_from_bq_to_avro
```

## (2) ga4_from_avro_to_sql

### build

```sh
cd 2_ga4_from_avro_to_sql
docker build ./ -t ga4_from_avro_to_sql
```

### run

- ga4_from_avro_to_sql.ini を実行ディレクトリにコピーして編集する。
- 以下を参考にして docker run を実行する。

```sh
docker run --mount type=bind,source="$(pwd)",target=/workdir ga4_from_avro_to_sql
```

## (3) ga4_from_sql_to_postgres

### build

```sh
cd 3_ga4_from_sql_to_postgres
docker build ./ -t ga4_from_sql_to_postgres
```

### run

- ga4_from_sql_to_postgres.ini を実行ディレクトリにコピーして編集する。
- 以下を参考にして docker run を実行する。

```sh
docker run --add-host=host.docker.internal:host-gateway --mount type=bind,source="$(pwd)",target=/workdir ga4_from_sql_to_postgres
```
