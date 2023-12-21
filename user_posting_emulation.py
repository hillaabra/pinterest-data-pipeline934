import random
from time import sleep

from posting_emulation_utils.aws_db_connector import AWSDBConnector
from posting_emulation_utils.multiprocess_data_sender import MultiprocessDataSender


new_connector = AWSDBConnector("posting_emulation_utils/aws_db_config.yaml")
pin_data = MultiprocessDataSender("0a0223c10829.pin", "streaming-0a0223c10829-pin", "pinterest_data")
geo_data = MultiprocessDataSender("0a0223c10829.geo", "streaming-0a0223c10829-geo", "geolocation_data", "timestamp")
user_data = MultiprocessDataSender("0a0223c10829.user", "streaming-0a0223c10829-user", "user_data", "date_joined")

data_senders = [pin_data, geo_data, user_data]

random.seed(100)

def run_infinite_post_data_loop():
    engine = new_connector.create_db_connector()
    while True:
        sleep(random.randrange(0, 2))
        random_row_number = random.randint(0, 11000)
        with engine.connect() as connection:
            for data_sender in data_senders:
                data_sender.post_random_record_to_batch_and_stream_layers(connection, random_row_number)


if __name__ == "__main__":
    run_infinite_post_data_loop()