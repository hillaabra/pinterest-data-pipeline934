# Pinterest Data Pipeline

*Data Engineering Project - [AiCore](https://www.theaicore.com/) (December 2023)*


![Static Badge](https://img.shields.io/badge/Skills%20%26%20Knowledge-A8B78B) ![Static Badge](https://img.shields.io/badge/Big%20data-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20ingestion-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Governance%20and%20Quality-8A2BE2) ![Static Badge](https://img.shields.io/badge/ETL-8A2BE2) ![Static Badge](https://img.shields.io/badge/Streaming-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20cloud-8A2BE2) ![Static Badge](https://img.shields.io/badge/Batch%20processing-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20requests-8A2BE2)

![Static Badge](https://img.shields.io/badge/Languages,%20Tools%20%26%20Libraries-A8B78B) ![Static Badge](https://img.shields.io/badge/Python-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20MSK-8A2BE2) ![Static Badge](https://img.shields.io/badge/Amazon%20EC2-8A2BE2) ![Static Badge](https://img.shields.io/badge/Apache%20Kafka-8A2BE2) ![Static Badge](https://img.shields.io/badge/IAM%20MSK%20Authentication-8A2BE2) ![Static Badge](https://img.shields.io/badge/MSK%20Connect-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20S3-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20Gateway-8A2BE2) ![Static Badge](https://img.shields.io/badge/Requests-8A2BE2) ![Static Badge](https://img.shields.io/badge/JSON-8A2BE2) ![Static Badge](https://img.shields.io/badge/YAML-8A2BE2) ![Static Badge](https://img.shields.io/badge/Command%20line-8A2BE2)

**The brief for this project was to create a version of Pinterest's experiment analytics data pipeline using the AWS Cloud. The pipeline enables Pinterest to crunch billions of datapoints per day in order to run thousands of experiments daily that provide valuable insights to improve the product.**


## Table of Contents
* [Project Overview](#project-overview)
* [File Structure](#file-structure)
* [Installation](#installation)
* [Usage](#usage)
* [Licence](#licence)

## Notes as I go along
- AiCore provided me with a Cloud log-in, with access to an IAM-authenticated MSK cluster that had already been created.
1. Batch Processing: Configuring te EC2 Kafka client
- I configured an EC2 instance to use as an Apache Kafka Client machine:
  - First, I connected to the EC2 instance that had been provided for me using the SSH client protocol.
  - Once I established a connection to the EC2 instance from the command line, I installed the packages I needed to onto the EC2 client machine in order to start communicating with the MSK cluster.
  - An EC2-access-role had been initialised for me in the IAM console - I edited the trust policy for this role in order to be able to assume it and gain the permissions to authenticate to the MSK cluster.
  - I configured my Kafka client to be able to use AWS IAM authentication by modifying the client.properties file.
  - Using the Bootstrap Servers String retrieved from the MSK Management console, I created three topics via the EC2 client machine:
    - <my_UserId>.pin for the Pinterest posts data
    - <my_UserId>.geo for the post geolocation data
    - <my_UserId>.user for the post user data
2. Batch Processing: Connecting the MSK cluster to a dedicated S3 bucket
- Next, within the MSK console, I created an MSK connector that would automatically send any data from Kafka topics going through the MSK cluster to an S3 bucket that had already been created for me:
  - An IAM role had already been written for me allowing me to write to the destination bucket, as well as a VPC endpoint to S3.
  - So the first thing I had to was to download the Confluent.io Amazon S3 Connector to the client EC2 machine I had initialised earlier, and then from the command line, I copied this sink connector to my designated S3 bucket.
  - I was then able to create a custom plugin in the MSK console containing the code to define the logic of the sink connector, using this S3 connector object.
- The next stage was to use this custom plugin to create the connector.
  - I configured the settings to make sure that the connector would pick up data from all three Kafka topics I had previously created (using the `topics.regex` field), and that it would send and store all that data in the correct bucket, in the correct region I had configured the bucket.
  - I made sure to choose the same IAM role I had previously used for authentication on my EC2 client, which also contained all the necessary permissions to connect to MSK and MSK Connect.
- I had to install PyMySQL for the AWSDBConnector class
3. Batch Processing: Configuring an API in API Gateway
- I craeted a resource in an API that had been assigned to me within API Gateway.
- Within this reource, I built a Kafka REST proxy integration HTTP ANY method, passing it an endpoint URL linking to the active machine of my EC2 instance. (I learnt the hard way that each time I stopped and relaunched the EC2 instance, a new publicDNS was generated, which I would have to update the API endpoint URL with to reflect.)
- After deploying the API, I set up the Kafka REST proxy on my EC2 client machine. I first had to install the Confluent package for the Kafka REST Proxy and then configure the REST proxy properties by adding in the correct bootstrap server and zookeeper connection strings for the MSK cluster, the correct port to match the port the API's URL endpoint was set to in API Gateway, as well as configuring it to perform IAM authentication to the MSK cluster.
- I wrote the code to send data to my Kafka topics in a Python script, creating DataSender classes that could be initialised by each topic name and source_table_name in the AWS RDS DB. The code emulates the production of real-time data, by choosing a record at random from each of these tables to send to the topics.
  - The data was sent as JSON. I wrote a method to convert the datetime values of the records to ISO strings - these were the only non-string or numeric values.
- After starting the REST proxy on the EC2 client, and debugging my AWS setings and kafka-rest config files, I was able to send a batch of data to the the MSK cluster by running my Python script from the terminal. The custom plug-in and connector I had created earlier in the MSK/MSK Connect console made sure that the topics I was sending to the cluster were distributed into their respect topic folders in the S3 bucket.