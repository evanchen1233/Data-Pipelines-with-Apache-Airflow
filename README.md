# Data Pipelines with Apache Airflow
## Introduction
The objective of this project is to construct a data pipeline for YouTube trending data using Python and Apache Airflow. Apache Airflow will be employed to manage the workflow, schedule events, and provide email notifications. We will use Python pandas dataframe for data transformation and MySQL connector load data into warehouse.
![architecture](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/bdef2ffa-9a8f-4ac2-8540-92b463456dbb)

## Project Phases
The provided DAG illustrates the tasks incorporated within the workflow, written in dag.py.
![Capture](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/be482174-4ee9-4cba-89e4-937110b4697a)

1. Extract data (Raw CSV and JSON files) from cloud storage.
2. Preprocess the data, including conversion of data types from Python objects to datetime, string, and integer formats. Remove duplication.
3. Construct 8 dimension tables by using pandas dataframes. Incorporate category name mapping from the JSON file.
4. Construct the fact table by using pandas dataframes. Add table relationship between fact table and dimension tables.
5. Load dimension tables and fact table into MySQL as the data warehouse.

![Capture1](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/612670bf-ad77-4ab6-a383-e74fce50d59b)
![Capture2](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/aabf1104-bba8-428f-a8f8-8e8da1ed06f3)
![Capture3](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/c0b46b0c-a197-4436-b27e-4aa97fe58e57)

7. Visualise the data with a dashboard 
![239732521-dd7b527c-f410-4290-82ca-a9d92e9947bc](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/b4a81f6d-d2c9-41d3-921a-8fe726b04ac4)

## Key Learning

1. Manage task dependency
2. Use Xcom(cross communication) to pass value between tasks.
3. Experience the Airflow's UI and different ways to trgger an event.
4. Configure the system to receive notifications for scheduler event execution, failures, or retries.
5. Learn how to convert pandas dataframe to sql table and load into mySql database. (mySql connector/engine)


## Dataset Used

This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

https://www.kaggle.com/datasets/datasnaek/youtube-new
