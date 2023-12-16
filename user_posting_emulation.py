from time import sleep
import random
from multiprocessing import Process # not used?

import boto3 # not used?

from aws_db_connector import AWSDBConnector
from batch_data_generator import BatchDataGenerator


new_connector = AWSDBConnector("aws_db_config.yaml")
pin_data = BatchDataGenerator("0a0223c10829.pin", "pinterest_data")
geo_data = BatchDataGenerator("0a0223c10829.geo", "geolocation_data", "timestamp")
user_data = BatchDataGenerator("0a0223c10829.user", "user_data", "date_joined")

data_generators_by_topic = [pin_data, geo_data, user_data]

random.seed(100)

def run_infinite_post_data_loop():
    engine = new_connector.create_db_connector()
    while True:
        sleep(random.randrange(0, 2))
        random_row_number = random.randint(0, 11000)
        with engine.connect() as connection:
            for data_generator in data_generators_by_topic:
                data_generator.send_random_record_to_topic(connection, random_row_number)

if __name__ == "__main__":
    run_infinite_post_data_loop()