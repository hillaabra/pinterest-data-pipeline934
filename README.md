# Pinterest Data Pipeline

*Data Engineering Project - [AiCore](https://www.theaicore.com/) (December 2023)*


![Static Badge](https://img.shields.io/badge/Skills%20%26%20Knowledge-A8B78B) ![Static Badge](https://img.shields.io/badge/Big%20data%20storage%20and%20analytics-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20ingestion-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20governance%20and%20quality-8A2BE2) ![Static Badge](https://img.shields.io/badge/ETL%20pipeline-8A2BE2) ![Static Badge](https://img.shields.io/badge/Stream%20processing-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20cloud-8A2BE2) ![Static Badge](https://img.shields.io/badge/Batch%20processing-8A2BE2) ![Static Badge](https://img.shields.io/badge/APIs-8A2BE2) ![Static Badge](https://img.shields.io/badge/REST%20proxy%20integration-8A2BE2) ![Static Badge](https://img.shields.io/badge/Lambda%20architecture-8A2BE2) ![Static Badge](https://img.shields.io/badge/Serverless%20computing-8A2BE2)

![Static Badge](https://img.shields.io/badge/Languages,%20Tools%20%26%20Libraries-A8B78B)  ![Static Badge](https://img.shields.io/badge/Python-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQL-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20MSK-8A2BE2) ![Static Badge](https://img.shields.io/badge/Amazon%20EC2-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Kafka-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Spark-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Airflow-8A2BE2) ![Static Badge](https://img.shields.io/badge/Databricks-8A2BE2) ![Static Badge](https://img.shields.io/badge/MWAA-8A2BE2) ![Static Badge](https://img.shields.io/badge/IAM%20MSK%20Authentication-8A2BE2) ![Static Badge](https://img.shields.io/badge/MSK%20Connect-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20Kinesis-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20S3-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20Gateway-8A2BE2) ![Static Badge](https://img.shields.io/badge/Requests-8A2BE2) ![Static Badge](https://img.shields.io/badge/JSON-8A2BE2) ![Static Badge](https://img.shields.io/badge/YAML-8A2BE2) ![Static Badge](https://img.shields.io/badge/Command%20line-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQLAlchemy-8A2BE2)


**The brief for this project was to build an end-to-end AWS-hosted data pipeline inspired by Pinterest’s experiment processing pipeline.**

**The pipeline is built to be able to crunch billions of datapoints per day in order to run thousands of experiments daily.**

**Apart from providing me with experience in setting up and implementing complex Cloud infrastructure, working with stream- and batch- processing methods in Databricks and job-orchestration using Airflow, this project helped me to get familiar with Lambda architecture as a powerful framework for the efficient processing of big data.**

**By leveraging the benefits of batch-processing for resource-intensive queries on historical data and the speed of stream-processing for low-intensity, low-latency production of real-time views, this hybrid deployment model offers an agile, efficient and high-fault-tolerant data-processing stack that enables consistent and targeted evaluation of Pinterest's product on an ongoing basis.**

## Table of Contents
* [Project Overview](#project-overview)
* [Next Steps](#next-steps)
* [File Structure](#file-structure)
    * [Local Machine](#local-machine)
    * [EC2 Client Machine](#ec2-client-machine)
    * [Databricks Workspace](#databricks-workspace)
    * [AWS MWAA Environment](#aws-mwaa-environment)
* [Installation](#installation)
* [Usage](#usage)
* [Licence](#licence)

## [Project Overview](#project-overview)

To mimic the creation of real-time user data from Pinterest, I wrote scripts that extract rows of data at random from three tables within an AWS RDS database. These randomly extracted datapoints are sent as JSON objects via an API (which I developed on AWS API Gateway) into their respective processing layers within the pipeline.

The pipeline is developed using a Lambda architecture.

For the **Batch Layer**:
- The data is ingested, in relation to three Kafka topics, via a REST proxy-integrated API connecting the Kafka Client launched on an EC2 instance with an MSK cluster on AWS.
- I created a sink connector within MSK Connect that directs the incoming data to its target topic folder within an S3 bucket.
- The data is processed within Databricks using Apache Spark: within Databricks, the data from each topic is read into Spark DataFrames and cleaned before being queried using SQL.
- Extracting comprehensive insights from the so-called historical data of the batch layer, the SQL queries generate daily, precomputed batch views as DataFrames, ready for ingestion to a **Serving Layer**.
- The job is orchestrated from the Apache Airflow UI on an AWS MWAA environment using a DAG which currently schedules the batch layer pipeline to be run once daily at midnight.

For the **Speed Layer** (or **Stream Layer**):
- The data is ingested, as three streams, using AWS API Gateway into AWS Kinesis.
- The data is read in near-real-time from Kinesis into DataFrames using Spark Structured Streaming within Databricks.
- After cleaning, the data is written into Databricks Delta Tables for long-term storage.

The same **data cleaning transformations** are performed on the corresponding datasets across the two layers. These include:
- Reordering, renaming, combining and/or dropping columns for better data comprehension
- Type-casting columns where necessary
- Data normalisation, including replacing missing or unmeaningful values with `None`

## [Next Steps](#next-steps)

The next stage of development for this pipeline would be to develop the **Server Layer** of the architecture, where the outputs of the batch and stream layers could be merged to allow for both historical and real-time data analysis. (To provide a faithful implementation of this, I would need to first amend the posting emulation scripts to generate the same datapoints concurrently for input to the pipeline.)

## [Installation](#installation)
Please note that if you are not a member of AiCore you will not be able to recreate this pipeline since it relies on access to private resources.

This project relies on access to a database on AWS storing data across three tables which resemble the data generated each time a user posts a post on Pinterest. The scripts I wrote extract rows of data at random from this database to simulate the creation of real-time user data, each of which sends this data to an API which directs this data into the data pipelines.

The following steps are needed before running the pipelines:

To run the posting emulation scripts from your local machine, you'll need the following packages installed on your local environment:
- Python 3+
- PyMySQL (if connecting to a MySQL database, as we are here)
- SQLAlchemy
- PyYAML

Clone this repository to your local machine using:
```
git clone ....
```
And you'll need to add the following files (see the file structure below for where to save these in the repository):
- `api_gateway_config.yaml` to store the invoke url of the API created on API Gateway
- `aws_db_config.yaml` to store the database connection details for the AWS-stored data from which we are extracting the simulated data points
- `<UserId>-key-pair.pem` to store the value for the key-pair used for SSH client authenticaion to your EC2 instance

(Refer to the file structure below to see which required files are hidden from this repository and where they should be saved.)

You will also need an AWS account with access to MSK, MSK Connect, API Gateway, S3, EC2 and MWAA services, and a Databricks account.

Before running any posting emulation scripts, the following steps need to be taken to establish the AWS Cloud Infrastructure that will facilitate these pipelines:

1. Create an Apache Kafka cluster in the Amazon MSK Console. Make a note of the region you created this in, and make sure all connected services are set up in the same region. (For this pipeline the region was `us-east-1`.)
2. Launch an EC2 instance that will act as your Apache Kafka client instance.
3. Generate and retrieve a key-pair to enable secure connection to your EC2 instance, saving the key-pair value within the `<UserId>-key-pair.pem` file you created on your local machine. (Make sure that the `.pem` filename matches the `Key pair name` associated with your key-pair on the AWS console.)
4. If needed, adjust the Security Group settings in your EC2's Virtual Private Cloud (whether default or pre-configured) to allow it to accept all traffic from the client machine.
5. Make sure your IAM role has access to the EC2 instance by creating and/or assuming the relevant access role with the necessary trust relationship.
7. Create a bucket within AWS S3 to serve as the destination for the data received from the connector.
8. To follow principles of least privilege, create an IAM role with the following policy:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::<DESTINATION_BUCKET>",
                "arn:aws:s3:::<DESTINATION_BUCKET>/*"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucketMultipartUploads",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "s3:ListAllMyBuckets",
            "Resource": "*"
        }
    ]
}
```
9. Allow `kafkaconnect.amazonaws.com` to assume the principal role for the policy.
10. Create a VPC gateway endpoint to S3.
11. Connect to your EC2 instance via your local machine, using the SSH Client connection protocol.
12. Inside the EC2 client machine, install Java and then Kafka (making sure to install the same version of Kakfa that your MSK cluster is running on).
> insert the commands here?
12. Install the IAM MSK Authentication package (available on GitHub) within the Kafka `libs` directory.
14. To enable the Kafka client to locate and utilize the necessary Amazon MSK IAM libraries when executing commands, add the path to the IAM MSK Authentication package to a CLASSPATH variable inside your EC2 instance's `/home/ec2-user/.bashrc` file and then run the file using the `source` command to implement the export command.
15. Configure your Kafka client to use AWS IAM authentication to the cluster by modifying the client.properties file inside the Kafka `bin` directory.
16. After retrieving the BootstrapServerString from the MSK Management console, create the following three Kafka topics:
    - <UserId>.pin for the Pinterest posts data
    - <UserId>.geo for the post geolocation data
    - <UserId>.user for the post user data
17. From the EC2 instance, download an S3 connector that can export data from Kafka topics to S3. This pipeline used Confluent.io's Amazon S3 Connector. You may need to assume admin user privileges before downloading your connector to a new directory on your client machine:
```
# assume admin user privileges
sudo -u ec2-user -i
# create directory in which to save the connector
mkdir kafka-connect-s3 && cd kafka-connect-s3
# download connector from Confluent
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
```
Your EC2 instance should now have the following directories - EITHER FINISH OR REMOVE THIS:
```
|-- kafka_2.12-2.8.1
    |-- libs
            |-
    |-- kafka-connect-s3
            |-- confluentic-kafka-connect-s3-10.0.3.zip

```
18. Copy the connector over to your S3 bucket:
```
aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
```
19. Back on the MSK console, under MSK Connect, create a custom plug-in selecting the S3 URI of the Confluent.io S3 Connector you copied over. For this project, the naming convention `<UserId>-plugin` was used.
20. Create a connector within MSK Connect using the plugin you have just created, setting the connector type to Provisioned, and the MCU count per worker to 1, and number of workers to 1, provide access permissions the to IAM role create previously, and configuring the following settings to make sure that the connector will pick up data from all three Kafka topics previously created and that it will send and store all that data in the correct bucket:
```
connector.class=io.confluent.connect.s3.S3SinkConnector
s3.region=us-east-1 # make sure to choose the same region as your bucket and cluster
flush.size=1
schema.compatibility=NONE
tasks.max=3
topics.regex=<UserId>.* # replace <UserID> with the userid you used to create the Kafka topics earlier
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<BUCKET_NAME> # insert the correct name of your S3 bucket
```
21. Create an API within API Gateway.
22. Create a resource within the API with a Kafka REST proxy integration ANY method, passing it an endpoint URL linking to the active machine of my EC2 instance. (I learnt the hard way that each time I stopped and relaunched the EC2 instance, a new publicDNS was generated, which I would have to update the API endpoint URL with to reflect.)
23. After deploying the API, set up the Kafka REST proxy on your EC2 client machine. You will first have to install the Confluent package for the Kafka REST Proxy and then configure the REST proxy properties by adding in the correct BootstrapServer and ZooKeeper connection strings for the MSK cluster, the correct port to match the port the API's URL endpoint is set to in API Gateway, as well as configuring it to perform IAM authentication to the MSK cluster:
```
add code?
```
24. Sign in to your Databricks account.
25. If your account doesn't already have access to S3, you will need to set this up by creating a user with `AmazonS3FullAccess` permissions. Under the Security Credentials for the user, create an access key (selecting AWS CLI authentication) and download the csv file for the programmatic key. Within the Databricks UI, create a table within the Data console, uploading the credentials file to create a table. Make a note of the filepath to the credentials CSV.
26. Upload to your Databricks workspace the files and folders contained within the `databricks_workspace` directory you previously cloned from this GitHub repository. (You can now delete these from your local machine.)
27. Make sure that the filepath to the AWS credentials table is correctly set. IN WHICH PLACES?
28. Back on the AWS S3 console, create another S3 bucket which is configured to block all public access and has Bucket Versioning enabled. Within this bucket, create a folder called `dags`.
29. Navigate to the AWS MWAA console, and create an environment in which to orchestrate your DAG workflows, configuring it to launch it with an MWAA VPC and choosing your desired Apache Airflow access mode (private network or public network). Create a new security group to allow the MWAA to have defined inbound and outbound rules based on the type of web server selected. Choose the smallest environment size as is necessary to support the workload.
30. Upload the
30. Once your MWAA environment has been created, select it and click on `Open Airflow UI` - w

## [File Structure](#file-structure)

### Databricks Workspace
![A code block representing the file structure of the databricks workspace](images/carbon-databricks.png)

- `batch_processing_pipeline.ipynb` conducts the processing of the batch layer data read from S3:
    - If not already mounted, the script mounts the S3 bucket to Databricks
    - It reads the JSON objects in S3 into a Spark dataframe for each topic
    - It cleans the data in the Spark dataframe (replacing erroneous or missing values with `None`s, data normalisation, type casting, dropping or creating new columns)
    - It then conducts a series of SQL queries on the cleaned datasets, which are stored to dataframes
    - In the next step for this project, these batch views would be output to a serving layer
- `utils`
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

To emulate the data generated by Pinterest, this project relies on access to an RDS database on AWS storing data across three tables which resemble the data generated each time a user posts something on Pinterest. The three datasets represent:
- Data relating to the Pinterest post itself (aka the `pin` table)
- Data about the user that interacted with the post (aka the `user` table)
- Data about the geolocation of the user-post interaction (aka the `geo` table)

The scripts I wrote extract rows of data at random from this database to simulate the creation of real-time user data, each of which sends this data to an API which directs the data into the target processing layer of the pipeline.

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