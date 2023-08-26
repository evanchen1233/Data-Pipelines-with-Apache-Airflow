# Data Pipelines with Apache Airflow
## Introduction
The objective of this project is to construct a data pipeline for YouTube trending data using Python and Apache Airflow. Apache Airflow will be employed to manage the workflow, schedule events, and provide email notifications.

## Project Phases
The provided DAG illustrates the tasks incorporated within the workflow.
![Capture](https://github.com/evanchen1233/Data-Pipelines-with-Apache-Airflow/assets/101177476/0c73064e-8bc6-4c2f-a0c7-5e53f5c8b09d)

1. Extract data from Raw CSV and JSON files.
2. Preprocess the data, including conversion of data types from Python objects to datetime, string, and integer formats. Remove duplication.
3. Construct 8 dimension tables by using pandas dataframes. Incorporate category name mapping from the JSON file.
4. Construct the fact table by using pandas dataframes.
5. Store the processed data in CSV files or MySQL as the data warehouse.

## Key Learning

1. Manage task dependency
2. Use Xcom(cross communication) to pass value between tasks.
3. Experience the Airflow's UI and different ways to trgger an event.
4. Configure the system to receive notifications for scheduler event execution, failures, or retries.

## Dataset Used

This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

https://www.kaggle.com/datasets/datasnaek/youtube-new
