# Airflow 

![alt-text](../../img/airflow.jpg)

## Table of contents
1. [General Idea](#general-idea)
2. [Instance Type and Architecture](#instance-type-and-architecture)
2. [Installation and Prerequisites](#installation-and-prerequisites)
2. [Scripts](#scripts)
    1. [spark_live_to_minute](#spark_live_to_minute)
    1. [spark_minute_to_hour](#spark_minute_to_hour)
    1. [spark_hour_to_day](#spark_hour_to_day)
    1. [spark_aggregations](#spark_aggregations)
3. [Future Improvements](#future-improvements)
    1. [Cassandra](#cassandra)
    2. [Terraform](#terraform)


## General Idea

We use Airflow to schedule Spark jobs that aggregate the data from Cassandra. There's two kinds of DAGs, 
that correspond to the different types of Spark jobs.
* **Time frame averages**: Cassandra contains data by second, by minute, by hour and by day. Spark aggregates
this data to the higher level by doing the mean of the values of the lower level. 
For example, to go from live to minute, we start with live data:

       
        2019-01-01_12-34-00
        2019-01-01_12-34-01
        ...
        2019-01-01_12-34-59

and transforms it into

        2019-01-01_12-34
        
* **Metadata aggregations**: Triggers the Spark job that aggregates daily data based on game, genre, console and language.

You can learn more about the Spark jobs [here](../spark/README.md).

## Instance Type and Architecture

Airflow runs on a dedicated m5.large instance with an Elastic IP and 80 GB of EBS. This instance only runs Airflow.
In terms of security groups, we need:
* SSH access
* Port 8080 open to access the Airflow Webserver
* Ability to SSH into the Spark Master

## Installation and Prerequisites

To install Airflow, simply follow the [instructions](https://airflow.apache.org/installation.html). 
Our implementation doesn't use any other database or executor as we don't need them for now.

In order to run the DAGs correctly, you will also need to set up a public RSA key between Airflow and Spark Master, so that
we can ssh between them without a need for a password. You can also modify the DAGs to establish a connection using a 
password if you prefer.

## Scripts

Each DAG (Python file) has associated a bash script that has to reside in the home folder of Spark Master. Every Python 
script has the same structure: 

* We establish the default arguments and the scheduling time.
* Set variables that define the SSH connection and the bash scripts to execute on Spark Master.
* Define the jobs that constitute the DAG.
* Describe the workflow (if any).

### spark_live_to_minute

* DAG: [spark_live_to_minute.py](spark_live_to_minute.py)
* Related Bash Script: [spark_live_to_minute.sh](spark_live_to_minute.sh)
* Frequency: Every minute except the HH:00.
* Number of tasks: 1
* Workflow: None

### spark_minute_to_hour

* DAG: [spark_minute_to_hour.py](spark_minute_to_hour.py)
* Related Bash Script: [spark_minute_to_hour.sh](spark_minute_to_hour.sh), 
[spark_live_to_minute.sh](spark_live_to_minute.sh)
* Frequency: Every hour except midnight.
* Number of tasks: 2
* Workflow: live_to_minute >> minute to hour

### spark_hour_to_day

* DAG: [spark_hour_to_day.py](spark_hour_to_day.py)
* Related Bash Script: [spark_hour_to_day.sh](spark_hour_to_day.sh), [spark_minute_to_hour.sh](spark_minute_to_hour.sh), 
[spark_live_to_minute.sh](spark_live_to_minute.sh), [spark_aggregations.sh](spark_aggregations.sh)
spark_aggregations.sh
* Frequency: Every day at midnight (calculates data from previous day)
* Number of tasks: 4
* Workflow: live_to_minute >> minute_to_hour >> hour_to_day >> aggregations

hour_to_day contains aggregations as well, since we require the daily data to be already calculated for the aggregations
to take place.

### spark_aggregations

* DAG: [spark_aggregations.py](spark_aggregations.py)
* Related Bash Script: [spark_aggregations.sh](spark_aggregations.sh)
* Frequency: Every day at midnight
* Number of tasks: 1
* Workflow: None

This DAGs is not supposed to run on schedule, but you can trigger a recalculation of the aggregations if the situation 
requires it, as well as calculate a backfill of the aggregation data without recalculating all the previous daily averages. 


## Future Improvements

### Cassandra
If we reimplement the pipeline as a streaming process, Airflow loses most of it's usefullness as a 
scheduler in this pipeline. There is however another use case for it, and that is creating and dropping Cassandra
materialized views for Flask access. Since materialized views essentially duplicate the data, being able to choose the 
subset of the tables that we need for our uses and recreating them at convenience would be an interesting way to go.

### Terraform

The second way we could implement Airflow is as a self-healing triggering event. Once all of the [Terraform](../terraform/)
scripts are done, we could use Airflow to constantly monitor that our clusters are healthy. In case any of the nodes is not
responding accordingly, we can spin up a replacement and terminate the deficient node. This would probably require 
implementing a new database for Airflow (rather than the default) as well as another Executor (like CeleryExecutor) rather
than the default SequentialExecutor.
