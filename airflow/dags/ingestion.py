import os
import logging

from os import listdir
from os.path import isfile, join

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

archive_root_url="https://parisdata.opendatasoft.com/api/datasets/1.0/comptages-routiers-permanents-historique/attachments/"
file_url="opendata_txt_{{ execution_date.strftime(\'%Y\') }}_zip"


all_vehicle_url="https://parisdata.opendatasoft.com/explore/dataset/comptage-multimodal-comptages/download/?format=csv&timezone=Europe/Paris&lang=fr&use_labels_for_header=true&csv_separator=%3B"


bike_url="https://parisdata.opendatasoft.com/explore/dataset/comptage-velo-donnees-compteurs/download/?format=csv&timezone=Europe/Paris&lang=fr&use_labels_for_header=true&csv_separator=%3B"


curr_file="https://parisdata.opendatasoft.com/explore/dataset/comptages-routiers-permanents/download/?format=csv&timezone=Europe/Paris&lang=fr&use_labels_for_header=true&csv_separator=%3B"


current_api="https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv?where=t_1h+%3E%3D+" +'{{ execution_date.strftime(\'%Y-%m\') }}'+ "&timezone=Europe/Paris&limit=-1"

bike_api="https://opendata.paris.fr/api/v2/catalog/datasets/comptage-velo-donnees-compteurs/exports/csv?where=t+%3E%3D+"+'{{ execution_date.strftime(\'%Y-%m\') }}'+ "&timezone=Europe/Paris&limit=-1"

alltraffic_api="https://opendata.paris.fr/api/v2/catalog/datasets/comptage-velo-donnees-compteurs/exports/csv?where=t+%3E%3D+"+'{{ execution_date.strftime(\'%Y-%m\') }}'+"&timezone=Europe/Paris&limit=-1"

download_folder=AIRFLOW_HOME+'/data/'
year = '{{ execution_date.strftime(\'%Y\') }}'
year_month = '{{ execution_date.strftime(\'%Y-%m\') }}'
zip_name='{{ execution_date.strftime(\'%Y\') }}.zip'
url_template=archive_root_url+file_url
GCS_PATH_TEMPLATE = "raw/{{ execution_date.strftime(\'%Y\') }}/"


# bike_schema_changed=pa.schema([
#     pa.field('id_compteur', pa.string()),
#     pa.field('nom_compteur', pa.string()),
#     pa.field('id', pa.int64()),    c
#     pa.field('name', pa.string()), 
#     pa.field('sum_counts', pa.float64()), 
#     pa.field('date', pa.timestamp('ns', tz=None)), 
#     pa.field('installation_date', pa.date32()), 
#     pa.field('url_photos_n1', pa.string()), 
#     pa.field('coordinates', pa.string()), 
#     pa.field('counter', pa.string()), 
#     pa.field('photos', pa.string()), 
#     pa.field('test_lien_vers_photos_d,u_site_de_comptage_', pa.string()), 
#     pa.field('id_photo_1', pa.string()),
#     pa.field('url_sites', pa.string()),    
#     pa.field('type_dimage', pa.string()) 
# ])

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


# def reformat_parquet(parquet_name, date_col,schema,src_dir,dest_file):
#     columns = []
#     my_columns = [date_col]
#     for column_name in {parquet_name}.column_names:
#         column_data = {parquet_name}[column_name]
#         if column_name in my_columns:
#             column_data = pa.array({parquet_name}[column_name].to_pandas().astype('datetime64'))
#         columns.append(column_data)

#     updated_table = pa.Table.from_arrays(
#         columns, 
#         schema={schema}
#         )       
#     pq.write_table(updated_table, src_dir+dest_file+".parquet")

def format_to_parquet(src_dir, info):
    onlyfiles = [f for f in os.listdir(src_dir) if (f.split("capteurs_")[-1].startswith(year) and f.endswith('.txt') )]
    for x in onlyfiles:
        dest_file = x.split("capteurs_")[-1].split(".txt")[0]
        table = pv.read_csv(download_folder+x, parse_options=pv.ParseOptions(delimiter=";"))
        pq.write_table(table, src_dir+dest_file+".parquet")

def format_to_parquet_current(src_dir, info):

    # I have a bug here I could not fix, the ingestion works if I run these lines in a notebook but with the DAG, it transforms the timestamp data as object
   
    table = pv.read_csv(src_dir+info+".csv", parse_options=pv.ParseOptions(delimiter=";"))
    pq.write_table(table, src_dir+info+".parquet")    
    # if info in ["bike", "all_vehicle"]:
    # if date_col:

    #     my_columns=[{date_col}]
    #     for column_name in table.column_names:
    #         column_data = table[column_name]
    #         if column_name in my_columns:
    #             column_data = pa.array(table[column_name].to_pandas().astype('datetime64'))
    #         columns.append(column_data)
    #     updated_table = pa.Table.from_arrays(
    #         columns, 
    #         schema={schema}
    #         )       
    #     pq.write_table(updated_table, src_dir+info+".parquet", use_deprecated_int96_timestamps=True)
    # else: 


def upload_to_gcs(bucket, local_folder,object_name, info):
    client = storage.Client()
    bucket = client.bucket(bucket)
    onlyfiles =[f for f in os.listdir(local_folder) if (f.startswith(info) and f.endswith('.parquet') ) ]
    for x in onlyfiles:
        blob = bucket.blob(object_name+x)
        blob.upload_from_filename(local_folder+x)




default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def donwload_parquetize_upload_dag(
    dag,
    url_template,
    download_folder,
    zip_name,
    gcs_path_template,
    info
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {download_folder}{zip_name} && unzip {download_folder}{zip_name} -d {download_folder}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_dir": download_folder, 
                "info":year            
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_folder": download_folder,
                "info":year  
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"find {download_folder} -name '*.zip' -type f -delete & find {download_folder} -name '*.txt' -type f -delete"
        )
        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

def donwload_parquetize_upload_current_dag(
    dag,
    url_template,
    download_folder,    
    gcs_path_template,
    info
):

    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task_current",
            bash_command=f"wget -O {download_folder}{info}.csv {url_template}"
            # bash_command=f"curl -sSLf '{url_template}' > {download_folder}{info}.csv"
            # bash_command=f"curl -sSLf {url_template}"
            # bash_command=f"curl -X 'GET' {api_v2} -o {download_folder}{year}.csv" 
            # bash_command=f"curl -X 'GET' {url_template} -o {download_folder}{year}.csv"
            # bash_command=f"curl -sSLf {url_template} -O {download_folder}{year}.csv"
        )
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task_current",
            python_callable=format_to_parquet_current,
            op_kwargs={
                "src_dir": download_folder,
                "info":info            
            },
        )        
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task_current",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_folder": download_folder,
                "info":info  
            },
        )
        rm_task = BashOperator(
            task_id="rm_task_current",
            bash_command=f"find {download_folder} -name '*.csv' -type f -delete"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task
        # download_dataset_task >> format_to_parquet_task >> local_to_gcs_task 


archives_dag = DAG(
    dag_id="archives",
    schedule_interval="0 0 1 2 *",
    start_date=datetime(2014, 1, 1),
    end_date=datetime(2020, 10, 10),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_dag(
    dag=archives_dag,
    url_template=url_template,
    download_folder=download_folder,   
    info=year,
    zip_name=zip_name,
    gcs_path_template=GCS_PATH_TEMPLATE
)

last_year_dag = DAG(
    dag_id="last_year",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 10, 10),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_dag(
    dag=last_year_dag,
    url_template=url_template+"_zip",
    download_folder=download_folder, 
    info=year,
    zip_name=zip_name,
    gcs_path_template=GCS_PATH_TEMPLATE
)


current= DAG(
    dag_id="current",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_current_dag(
    dag=current,
    url_template=curr_file,
    download_folder=download_folder,
    info=year,
    gcs_path_template="current/{{ execution_date.strftime(\'%Y\') }}/"
)



api_current= DAG(
    dag_id="api",
    schedule_interval="@monthly",
    start_date=days_ago(31),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
) 

donwload_parquetize_upload_current_dag(
    dag=api_current,
    url_template=current_api,
    download_folder=download_folder,  
    info=year_month,
    gcs_path_template="current/{{ execution_date.strftime(\'%Y-%m\') }}/"
)

bike= DAG(
    dag_id="bike",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_current_dag(
    dag=bike,
    url_template=bike_url,
    download_folder=download_folder,
    info="bike",
    gcs_path_template="bike/"
)

bike_api= DAG(
    dag_id="bike",
    schedule_interval="@monthly",
    start_date=days_ago(31),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_current_dag(
    dag=bike_api,
    url_template=bike_api,
    download_folder=download_folder,
    info="bike",
    gcs_path_template="bike/"
)



all_vehicle= DAG(
    dag_id="all_vehicle",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_current_dag(
    dag=all_vehicle,
    url_template=all_vehicle_url,
    download_folder=download_folder,
    info="all_vehicle",
    gcs_path_template="all_vehicle/"
)

all_vehicle_api= DAG(
    dag_id="all_vehicle",
    schedule_interval="@monthly",
    start_date=days_ago(31),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-greg-proj'],
)

donwload_parquetize_upload_current_dag(
    dag=all_vehicle_api,
    url_template=alltraffic_api,
    download_folder=download_folder,
    info="all_vehicle",
    gcs_path_template="all_vehicle/"
)






# api is:
# https://opendata.paris.fr/api/v2/catalog/datasets/comptage-velo-donnees-compteurs/exports/csv?where=date%3Ddate%272022-03-20%27&limit=-1&offset=0&refine=&timezone=UTC 
# need to pick several days or month cause > does not work