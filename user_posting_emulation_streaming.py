# In this script, you should send requests to your API, which adds one record at a time to the streams you have created.
# You should send data from the three Pinterest tables to their corresponding Kinesis stream.
import random
from time import sleep

from aws_db_connector import AWSDBConnector
from streaming_data_generator import StreamingDataGenerator


new_connector = AWSDBConnector("aws_db_config.yaml")
pin_data = StreamingDataGenerator("streaming-0a0223c10829-pin", "pinterest_data")
geo_data = StreamingDataGenerator("streaming-0a0223c10829-geo", "geolocation_data", "timestamp")
user_data = StreamingDataGenerator("streaming-0a0223c10829-user", "user_data", "date_joined")

data_generators_by_stream = [pin_data, geo_data, user_data]

random.seed(100)

def run_infinite_post_data_to_kinesis_loop():
    engine = new_connector.create_db_connector()
    while True:
        sleep(random.randrange(0, 2))
        random_row_number = random.randint(0, 11000)
        with engine.connect() as connection:
            for data_generator in data_generators_by_stream:
                http_response_status_code = data_generator.send_random_record_to_stream(connection, random_row_number)
                if http_response_status_code != 200:
                    print(f"Stream interrupted due to response.status_code for stream {data_generator.stream_name}: {http_response_status_code}")
                    break
                else:
                    print(f"Data sent to {data_generator.stream_name} stream...")

if __name__ == "__main__":
    run_infinite_post_data_to_kinesis_loop()