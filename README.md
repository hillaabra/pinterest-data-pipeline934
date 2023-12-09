# Pinterest Data Pipeline

*Data Engineering Project - [AiCore](https://www.theaicore.com/) (December 2023)*


![Static Badge](https://img.shields.io/badge/Skills%20%26%20Knowledge-A8B78B) ![Static Badge](https://img.shields.io/badge/Big%20data-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20ingestion-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Governance%20and%20Quality-8A2BE2) ![Static Badge](https://img.shields.io/badge/ETL-8A2BE2) ![Static Badge](https://img.shields.io/badge/Streaming-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20cloud-8A2BE2) ![Static Badge](https://img.shields.io/badge/Batch%20processing-8A2BE2)

![Static Badge](https://img.shields.io/badge/Languages,%20Tools%20%26%20Libraries-A8B78B) ![Static Badge](https://img.shields.io/badge/Python-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20MSK-8A2BE2) ![Static Badge](https://img.shields.io/badge/Amazon%20EC2-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Kafka-8A2BE2) ![Static Badge](https://img.shields.io/badge/IAM%20MSK%20Authentication-8A2BE2) ![Static Badge](https://img.shields.io/badge/MSK%20Connect-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20S3-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20Gateway-8A2BE2) ![Static Badge](https://img.shields.io/badge/Command%20line-8A2BE2)

**The brief for this project was to create a version of Pinterest's experiment analytics data pipeline using the AWS Cloud. The pipeline enables Pinterest to crunch billions of datapoints per day in order to run thousands of experiments daily that provide valuable insights to improve the product.**


## Table of Contents
* [Project Overview](#project-overview)
* [File Structure](#file-structure)
* [Installation](#installation)
* [Usage](#usage)
* [Licence](#licence)

## Notes as I go along
- AiCore provided me with a Cloud log-in, with access to an IAM-authenticated MSK cluster that had already been created.
- I configured an EC2 instance to use as an Apache Kafka Client machine:
  - First, I connected to the EC2 instance that had been provided for me using the SSH client protocol.
  - Once I established a connection to the EC2 instance from the command line, I installed the packages I needed to onto the EC2 client machine in order to start communicating with the MSK cluster.
  - An EC2-access-role had been initialised for me in the IAM console - I edited the trust policy for this role in order to be able to assume it and gain the permissions to authenticate to the MSK cluster.
  - I configured my Kafka client to be able to use AWS IAM authentication by modifying the client.properties file.
  - Using the Bootstrap Servers String retrieved from the MSK Management console, I created three topics via the EC2 client machine:
    - <my_UserId>.pin for the Pinterest posts data
    - <my_UserId>.geo for the post geolocation data
    - <my_UserId>.user for the post user data
- Next, within the MSK console, I created an MSK connector that would automatically send any data from Kafka topics going through the MSK cluster to an S3 bucket that had already been created for me:
  - An IAM role had already been written for me allowing me to write to the destination bucket, as well as a VPC endpoint to S3.
  - So the first thing I had to was to download the Confluent.io Amazon S3 Connector to the client EC2 machine I had initialised earlier, and then from the command line, I copied this sink connector to my designated S3 bucket.
  - I was then able to create a custom plugin in the MSK console containing the code to define the logic of the sink connector, using this S3 connector object.
- The next stage was to use this custom plugin to create the connector.
  - I configured the settings to make sure that the connector would pick up data from all three Kafka topics I had previously created (using the `topics.regex` field), and that it would send and store all that data in the correct bucket, in the correect region I had configured the bucket.
  - I made sure to choose the same IAM role I had previously used for authentication on my EC2 client, which also contained all the necessary permissions to connect to MSK and MSK Connect.
