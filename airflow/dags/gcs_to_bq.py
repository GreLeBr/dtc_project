import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'traffic_all')

DATASET = "traffic_all"
# COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# # NOTE: DAG declaration - using a Context Manager (an implicit way)
# with DAG(
#     dag_id="gcs_2_bq_dag",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=['dtc-de'],
# ) as dag:

#     # for colour, ds_col in COLOUR_RANGE.items():
#     # for year in range (2014, 2022):
#         # move_files_gcs_task = GCSToGCSOperator(
#         #     task_id=f'move_{colour}_{DATASET}_files_task',
#         #     source_bucket=BUCKET,
#         #     source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
#         #     destination_bucket=BUCKET,
#         #     destination_object=f'{colour}/{colour}_{DATASET}',
#         #     move_object=True
#         # )

#         bigquery_external_table_task = BigQueryCreateExternalTableOperator(
#             task_id=f"bq_external_table_task",
#             table_resource={
#                 "tableReference": {
#                     "projectId": PROJECT_ID,
#                     "datasetId": BIGQUERY_DATASET,
#                     "tableId": f"traffic_routier_external_table",
#                 },
#                 "externalDataConfiguration": {
#                     "autodetect": "True",
#                     "sourceFormat": f"{INPUT_FILETYPE.upper()}",
#                     "sourceUris": [f"gs://{BUCKET}/{INPUT_PART}/*"],
#                 },
#             },
#         )

#         CREATE_BQ_TBL_QUERY = (
#             f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.traffic_routier_table \
#             PARTITION BY DATE(t_1h) \
#             AS \
#             SELECT * FROM {BIGQUERY_DATASET}.traffic_routier_external_table;"
#         )

#         # Create a partitioned table from external table
#         bq_create_partitioned_table_job = BigQueryInsertJobOperator(
#             task_id=f"bq_create_partitioned_table_task",
#             configuration={
#                 "query": {
#                     "query": CREATE_BQ_TBL_QUERY,
#                     "useLegacySql": False,
#                 }
#             }
#         )

#         bigquery_external_table_task >> bq_create_partitioned_table_job


# all_vehicle_schema_changed=pa.schema([
#     pa.field('id_trajectoire', pa.string()),
#     pa.field('id_site', pa.string()),
#     pa.field('label', pa.int64()),    
#     pa.field('t', pa.timestamp('ns', tz=None)), 
#     pa.field('mode', pa.string()), 
#     pa.field('nb_usagers', pa.float32()), 
#     pa.field('voie', pa.string()), 
#     pa.field('sens', pa.string()), 
#     pa.field('trajectoire', pa.string()), 
#     pa.field('coordonnees_geo', pa.string())  
# ])

# all_vehicle_schema_changed=

# id_trajectoire:STRING, id_site:INTEGER,label:INTEGER, t:TIMESTAMP, mode:STRING,
# nb_usagers:STRING, voie:STRING, sens:STRING, trajectoire:STRING, coordonnees_geo:STRING 

# [{"name": "id_trajectoire", "type", "STRING", "mode": "NULLABLE"},
# {"name": "id_site", "type", "STRING", "mode": "NULLABLE"},
# {"name": "label", "type", "STRING", "mode": "NULLABLE"},
# {"name": "t", "type", "TIMESTAMP", "mode": "NULLABLE"},
# {"name": "mode", "type", "STRING", "mode": "NULLABLE"},
# {"name": "nb_usagers", "type", "STRING", "mode": "NULLABLE"},
# {"name": "voie", "type", "STRING", "mode": "NULLABLE"},
# {"name": "sens", "type", "STRING", "mode": "NULLABLE"},
# {"name": "trajectoire", "type", "STRING", "mode": "NULLABLE"},
# {"name": "coordonnees_geo", "type", "STRING", "mode": "NULLABLE"}]



                    #     "schema_fields":[
                    #     {"name": "id_trajectoire", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "id_site", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "label", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "t", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    #     {"name": "mode", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "nb_usagers", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "voie", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "sens", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "trajectoire", "type": "STRING", "mode": "NULLABLE"},
                    #     {"name": "coordonnees_geo", "type": "STRING", "mode": "NULLABLE"}],
                    # },

def upload_to_gcs_partion(
    dag,
    tableId,
    input_part,
    date_col,
    table
):

    with dag:
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id="bq_external_table_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": tableId,
                    },
                    "externalDataConfiguration": {
                        "autodetect": "True",
                        "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                        "sourceUris": [f"gs://{BUCKET}/{input_part}/*"]                        
                    },
                },
            )

            CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{table} \
                PARTITION BY DATE({date_col}) \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.{tableId};"
            )

            # Create a partitioned table from external table
            bq_create_partitioned_table_job = BigQueryInsertJobOperator(
                task_id="bq_create_partitioned_table_task",
                configuration={
                    "query": {
                        "query": CREATE_BQ_TBL_QUERY,
                        "useLegacySql": False,
                    }
                }
            )

            bigquery_external_table_task >> bq_create_partitioned_table_job


bike_dag=DAG(
    dag_id="bike_dag_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'])

upload_to_gcs_partion(
    dag=bike_dag,
    tableId='traffic_bike_external_table', 
    input_part="bike",
    date_col='date',
    table='traffic_bike_table'
)

traffic_dag=DAG(
    dag_id="traffic_dag_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'])

upload_to_gcs_partion(
    dag=traffic_dag,
    tableId='traffic_routier_external_table', 
    input_part="raw",
    date_col='t_1h',
    table='traffic_routier_table'
)

traffic_allvehicle=DAG(
    dag_id="traffic_allvehicle_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'])

upload_to_gcs_partion(
    dag=traffic_allvehicle,
    tableId='traffic_allvehicles_external_table', 
    input_part="all_vehicle",
    date_col='t',
    table='traffic_allvehicle_table'
)

current=DAG(
    dag_id="current_gcs",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'])

upload_to_gcs_partion(
    dag=current,
    tableId='current_external_table', 
    input_part="current",
    date_col='t_1h',
    table='current_table'
)