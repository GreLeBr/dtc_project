# DTC Data engineering project Grégoire

This repo is project as part of the Data Talks Club data engineering course available here:
https://github.com/DataTalksClub/data-engineering-zoomcamp


![Alt text](assets/01.jpg?raw=true  "Dashboard 1"){ width=50% }
![Alt text](assets/02.jpg?raw=true "Dashboard 2"){ width=50% }

## Table of Contents
* [1. About the project](#about-project)
* [2. Instructions](#instructions)

<a id='about-project'></a>
## 1. About the Project

The goal of this project was to gather public data publish on the Paris public data portal at :
https://parisdata.opendatasoft.com/pages/home/  

and attempt to crossrefer them for a better understanding of the mobility foodprint at a given time and place.   

I am using 3 data sources: 

    - One is measurement of car traffic, tracking cars per hour and road occupation
    https://parisdata.opendatasoft.com/explore/dataset/comptages-routiers-permanents/information/?disjunctive.libelle&disjunctive.etat_trafic&disjunctive.libelle_nd_amont&disjunctive.libelle_nd_aval

    - One is bike counting data
    https://parisdata.opendatasoft.com/explore/dataset/comptage-velo-donnees-compteurs/information/?disjunctive.id_compteur&disjunctive.nom_compteur&disjunctive.id&disjunctive.name 

    - The last one is a multi-modal counting of the traffic using image recognition
    https://parisdata.opendatasoft.com/explore/dataset/comptage-multimodal-comptages/information/?disjunctive.label&disjunctive.mode&disjunctive.voie&disjunctive.sens&disjunctive.trajectoire 


My goal was to bring these sources of data together by clustering the results in places where the sensors are as close as possible and compare the 3 different measurements.  

The multimodal cameras are less spread in Paris than the bike or car sensors, as such there was only one zone where the three sensors were close to each other but the the multimodal sensors is still about 200m away from a bike or a car sensor which makes the information not as precise I was hoping to get.  
Since the camera capture information from both cars and bikes, as well as pedestrians and other type of vehicle, it was mostly use to compare the data anyway.  

I have conducted the same approach between the bike and car sensors as there are multiple occurences of them being only 50m apart in Paris, leading to a much more relevant idea of how they relate to each others. 

Similarly I have identified car sensors close to multimodal camera but the data is not presented here.   

Car data has a lot of historical records which were fetch by airflow to be assembled in a bigquery database, however most of it predate the bike and multimodal sensors and ended up not being used for now. It might serve me for other analysis in the future. 
The dag runs yearly as a new data dump is made available, there is an issue with 2021 file which is poorly formated and I had to adapt my code accordingly. It will probably be necessary to tweak it for the year to come.  

The three other data sources were fetched once using the accumulated data made available and a DAG was made for each of them to fetch more data on a weekly/monthly basis to append to the original data.  

Data was parquetize, move to a data lake on google cloud before being transfered to a biquery database using airflow.  

I set up several queries using DBT to first calculate the proximity of sensors to each others and come with lists that identified the place where the 3 of them were the closest together as well as places where 2 sensors of different type are less than 50m away.  

To prepare the data information, I reduced granularity of bike data summing both directions that are otherwise individually recorded. The multimodal camera is not only able to recognize and classify the type of vehicle but also gather information on their trajectory, I reduced that aspect of the data for simplification accordingly to the problem at hand. 

The data is presented through 2 dashboards, one being information regarding the 3 sensors in close proximity to each others which ended up being close to Pl. de Clichy, 75018 Paris, France. 
There is another putative spot at 64 Rue de Rivoli but I did not use it. 

The other dashboard presents relationships between bike and car sensors data. 


Dashboards at: 
https://datastudio.google.com/reporting/b53d8cc4-588a-4388-9533-1aad366e1806
https://datastudio.google.com/reporting/0fd9aea9-14ac-4cb0-b35e-14e6b0c89114


<a id='instructions'></a>
## 2. Instructions

Fetching data is done using a docker container running either locally or from a virtual machine. 

To install a virtual machine, you can follow instructions from Alexey Grigorev
https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12 
Here are my personal notes that summarized the steps
https://trapezoidal-pin-b9e.notion.site/Installing-a-virtual-machine-on-google-3731661c395041babbe4dc4fde369e29


I have used google cloud for my project. https://console.cloud.google.com/

First set up a new account if you don't have one. 

Then set up a project and follow instructions to set up roles. 

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md#initial-setup


For convenience purpose, the google credentials are moved to directory ~/.google/credentials/google-credentials.json


To set up the project I used terraform with the appropriate variable related to the project ID. 

Instructions are available here

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform


Once your virtual machine/environment is setup:

Start by building the docker image:

```
docker-compose build
```
Initialize 
```
docker-compose up airflow-init
```

Run the image
```
docker-compose up
```

More informations available here
https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_2_data_ingestion/airflow


Once all the services are started, connect to airflow using http://localhost:8080/ 

Run the dags that need to be ran once, starting with the ingestion scripts

Then run the transfer from google cloud to biquery. 


The second part is done using DBT cloud
https://www.getdbt.com/  

You can setup a free account for single developper. 

Best is to follow the video serie to understand the process

I set up a single job, running

```
dbt run --var 'is_test_run: false'
```

I am planning to add tests potentially later and improve documentation on dbt. 

The job is run monthly to match the monthly feed of DAGS on new data. 

