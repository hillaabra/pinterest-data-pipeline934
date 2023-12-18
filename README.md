# Pinterest Data Pipeline

*Data Engineering Project - [AiCore](https://www.theaicore.com/) (December 2023)*


![Static Badge](https://img.shields.io/badge/Skills%20%26%20Knowledge-A8B78B) ![Static Badge](https://img.shields.io/badge/Big%20data-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20ingestion-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Governance%20and%20Quality-8A2BE2) ![Static Badge](https://img.shields.io/badge/ETL-8A2BE2) ![Static Badge](https://img.shields.io/badge/Streaming-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20cloud-8A2BE2) ![Static Badge](https://img.shields.io/badge/Batch%20processing-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20requests-8A2BE2)

![Static Badge](https://img.shields.io/badge/Languages,%20Tools%20%26%20Libraries-A8B78B) ![Static Badge](https://img.shields.io/badge/Python-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20MSK-8A2BE2) ![Static Badge](https://img.shields.io/badge/Amazon%20EC2-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Kafka-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Spark-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Airflow-8A2BE2) ![Static Badge](https://img.shields.io/badge/Databricks-8A2BE2) ![Static Badge](https://img.shields.io/badge/MWAA-8A2BE2) ![Static Badge](https://img.shields.io/badge/IAM%20MSK%20Authentication-8A2BE2) ![Static Badge](https://img.shields.io/badge/MSK%20Connect-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20S3-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20Gateway-8A2BE2) ![Static Badge](https://img.shields.io/badge/Requests-8A2BE2) ![Static Badge](https://img.shields.io/badge/JSON-8A2BE2) ![Static Badge](https://img.shields.io/badge/YAML-8A2BE2) ![Static Badge](https://img.shields.io/badge/Command%20line-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQLAlchemy-8A2BE2)

**The brief for this project was to create a version of Pinterest's experiment analytics data pipeline using the AWS Cloud. The pipeline enables Pinterest to crunch billions of datapoints per day in order to run thousands of experiments daily that provide valuable insights to improve the product.**


## Table of Contents
* [Project Overview](#project-overview)
* [File Structure](#file-structure)
* [Installation](#installation)
* [Usage](#usage)
* [Licence](#licence)

## Notes as I go along
- AiCore provided me with a Cloud log-in, with access to an IAM-authenticated MSK cluster that had already been created.
### Batch Processing
1. Configuring te EC2 Kafka client
- I configured an EC2 instance to use as an Apache Kafka Client machine:
  - First, I connected to the EC2 instance that had been provided for me using the SSH client protocol.
  - Once I established a connection to the EC2 instance from the command line, I installed the packages I needed to onto the EC2 client machine in order to start communicating with the MSK cluster.
  - An EC2-access-role had been initialised for me in the IAM console - I edited the trust policy for this role in order to be able to assume it and gain the permissions to authenticate to the MSK cluster.
  - I configured my Kafka client to be able to use AWS IAM authentication by modifying the client.properties file.
  - Using the Bootstrap Servers String retrieved from the MSK Management console, I created three topics via the EC2 client machine:
    - <my_UserId>.pin for the Pinterest posts data
    - <my_UserId>.geo for the post geolocation data
    - <my_UserId>.user for the post user data
2. Connecting the MSK cluster to a dedicated S3 bucket
- Next, within the MSK console, I created an MSK connector that would automatically send any data from Kafka topics going through the MSK cluster to an S3 bucket that had already been created for me:
  - An IAM role had already been written for me allowing me to write to the destination bucket, as well as a VPC endpoint to S3.
  - So the first thing I had to do was to download the Confluent.io Amazon S3 Connector to the client EC2 machine I had initialised earlier, and then from the command line, I copied this sink connector to my designated S3 bucket.
  - I was then able to create a custom plugin in the MSK console containing the code to define the logic of the sink connector, using this S3 connector object.
- The next stage was to use this custom plugin to create the connector.
  - I configured the settings to make sure that the connector would pick up data from all three Kafka topics I had previously created (using the `topics.regex` field), and that it would send and store all that data in the correct bucket, in the correct region I had configured the bucket.
  - I made sure to choose the same IAM role I had previously used for authentication on my EC2 client, which also contained all the necessary permissions to connect to MSK and MSK Connect.

3. Configuring an API in API Gateway
- I craeted a resource in an API that had been assigned to me within API Gateway.
- Within this reource, I built a Kafka REST proxy integration HTTP ANY method, passing it an endpoint URL linking to the active machine of my EC2 instance. (I learnt the hard way that each time I stopped and relaunched the EC2 instance, a new publicDNS was generated, which I would have to update the API endpoint URL with to reflect.)
- After deploying the API, I set up the Kafka REST proxy on my EC2 client machine. I first had to install the Confluent package for the Kafka REST Proxy and then configure the REST proxy properties by adding in the correct bootstrap server and zookeeper connection strings for the MSK cluster, the correct port to match the port the API's URL endpoint was set to in API Gateway, as well as configuring it to perform IAM authentication to the MSK cluster.
- I wrote the code to send data to my Kafka topics in a Python script, creating DataSender classes that could be initialised by each topic name and source_table_name in the AWS RDS DB. The code emulates the production of real-time data, by choosing a record at random from each of these tables to send to the topics.
  - The data was sent as JSON. I wrote a method to convert the datetime values of the records to ISO strings - these were the only non-string or numeric values.
- After starting the REST proxy on the EC2 client, and debugging my AWS setings and kafka-rest config files, I was able to send a batch of data to the the MSK cluster by running my Python script from the terminal. The custom plug-in and connector I had created earlier in the MSK/MSK Connect console made sure that the topics I was sending to the cluster were distributed into their respect topic folders in the S3 bucket.
4. Mounting S3 Bucket to Databricks
- Within a Notebook in Databricks, I mounted my designated S3 bucket which had received 2700 json records to Databricks.
- I read each topic grouping of JSON files to a Spark dataframe.
5. Spark on Databricks
- Within the Notebook in DataBricks, I wrote a class of methods to connect to S3, and a class of methods to process the batches of data by topic.
- I wrote custom methods to clean the data for each dataset, which could be passed into the data cleaning class method
- I cleaned the data, and ran sql queries on them by creating temporary tables views of the dataframes within the spark session native to the Spark cluster.
- I learnt how to run notebooks inside of notebooks in databricks
- version control?
6. AWS MWAA
- I wrote a DAG that triggered my Databricks notebook to run on a daily schedule, executing the batch processing pipeline at midnight each day.
- My AWS account had already been provided with access to an MWAA environment and its S3 bucket - i uploaded my DAG to the environment's S3 bucket, and then triggerred my DAG manually from the Airflow UI within the MWAA console to check it was running successfully.
- QUESTION: Should I have separated out the queries from this notebook? Should the query results have been stored somewhere?

### Stream Processing
1. AWS Kinesis
- I created three streaming topics within the console
- Then I added resources and methods to the API within the API Gateway Console to be able to post records to different data streams in Kinesis
- I deployed the amended API under a different stage name - TODO: test that the batch processing works on the newly deployed API and if so, have them both run on the same stage name (and amend in YAML/class methods)
- Then I defined the Python script for generating and sending that data to Kinesis (at this point I created an abstract base class Data Generator so that I wouldn't have to repeat the code)
- I tested it and it worked - I could see the data appearing in the shards of my Kinesis data streams.
- The next steps will be to read, transform and write the data to delta tables within DataBricks...

## Installation
Please note that if you are not a member of AiCore you will not be able to recreate this pipeline since it relies on access to private resources.

This project relies on access to a database on AWS storing data across three tables which resemble the data generated each time a user posts a post on Pinterest. The scripts I wrote extract rows of data at random from this database to simulate the creation of real-time user data, each of which sends this data to an API which directs this data into the data pipelines.

Refer to the file structure below to see which required files are hidden from this repository.

The following steps are needed before running the pipelines:

To run the posting emulation scripts from your local machine, you'll need the following packages installed on your local environment:
- Python 3+
- PyMySQL (if connecting to a MySQL database, as we are here)
- SQLAlchemy
- PyYAML

And you'll need to add the following files (see the file structure below for where to save these in the repository):
- `api_gateway_config.yaml` to store the invoke url of the API created on API Gateway
- `aws_db_config.yaml` to store the database connection details for the AWS-stored data from which we are extracting the simulated data points
- `<UserId>-key-pair.pem` to store the value for the key-pair used for SSH client authenticaion to your EC2 instance

You will also need an AWS account with access to MSK, MSK Connect, API Gateway, S3, EC2 and MWAA services.

Before running any posting emulation scripts, the followiwng steps need to be taken to establish the AWS Cloud Infrastructure that will facilitate these pipelines:

1. Create an Apache Kafka cluster in the Amazon MSK Console.
2. Launch an EC2 instance that will act as your Apache Kafka client instance.
3. Generate and retrieve a key-pair to enable secure connection to your EC2 instance, saving the key-pair value within the `<UserId>-key-pair.pem` file you created on your local machine. (Make sure that the `.pem` filename matches the `Key pair name` associated with your key-pair on the AWS console.)
4. If needed, adjust the Security Group settings in your EC2's Virtual Private Cloud (whether default or pre-configured) to allow it to accept all traffic from the client machine.
5. Make sure your IAM role has access to the EC2 instance by creating and/or assuming the relevant access role with the necessary trust relationship.
6.


### Setting up your EC2 Client Machine
5. Connect to your EC2 instance via your local machine, using the SSH Client connection protocol.
6. Inside the EC2 client machine, install Java and then Kafka.
7. Install the IAM MSK Authentication package (available on GitHub) within the Kafka `libs` directory.
8. To enable the Kafka client to locate and utilize the necessary Amazon MSK IAM libraries when executing commands, add the path to the IAM MSK Authentication package to a CLASSPATH variable inside your EC2 instance's `/home/ec2-user/.bashrc` file and then run the file using the `source` command to implement the export command.
9. Your EC2 instance should now have the following directories:
```
|-- kafka_2.12-2.8.1
    |-- libs
          '-

```

## File structure
Manually created the following
```
# Local Machine
|-- posting_emulation_utils/
    |-- __init__.py
    |-- api_gateway_config.yaml # hidden
    |-- aws_db_config.yaml # hidden
    |-- aws_db_connector.py
    |-- batch_data_generator.py
    |-- data_generator.py
    '-- streaming_data_generator.py
|-- <UserId>-key-pair.pem # hidden
|-- user_posting_emulation.py
'-- user_posting_emulation_streaming.py

# DataBricks WorkSpace
|-- // still to describe after tidying up

# S3
'-- user-<UserId>-bucket/


|-- dags/
    |-- 0a3db223d459_dag.py
|-- databricks_notebooks/
    |-- access_keys.ipynb
    |-- data_cleaning_tools.ipynb
    |-- data_cleaning.ipynb
    |-- data_sql_query.ipynb
    |-- mount_s3_to_databricks.ipynb
    |-- stream_and_clean_kinesis_data.ipynb

```
S3 after downloading Confluent.io Amazon S3 Connector and copying it to s3 bucket via the EC2 instance
```
|-- user-<UserId>-bucket/
    |-- kafka-connect-s3/
        '-- confluentinc-kafka-connect-s3-10.0.3.zip
    '-- topics/
```
S3 after data REST proxy is activated and data is sent
```
|-- user-<UserId>-bucket/
    |-- kafka-connect-s3/
        '-- confluentinc-kafka-connect-s3-10.0.3.zip
    '-- topics/
          |-- <UserId>.geo/
              '-- partition=0/
                  |-- JSON OBJECTS
                  '-- ...
          |-- <UserId>.pin/
              '-- partition=0/
                  |-- JSON OBJECTS
                  '-- ...
          '-- <UserId>.user/
              '-- partition=0/
                  |-- JSON OBJECTS
                  '-- ...
```

## Usage

To send data to Kafka topics
```
python -m user_posting_emulation
```

To send data to kinesis streams
```
python -m user_posting_emulation_streaming
```
## [Licence](#licence)

This project was supervised and is owned by [AiCore](https://www.theaicore.com/), a specialist AI & Data career accelerator whose focus is on building experience through real-world, industry-grade projects and applications.