import random
from multiprocessing import Event, Process
from time import sleep

from posting_emulation_utils.aws_db_connector import AWSDBConnector
from posting_emulation_utils.data_sender import DataSender

import concurrent.futures


new_connector = AWSDBConnector("posting_emulation_utils/aws_db_config.yaml")

def worker(data_sender, stop_event, random_number_generator):

    engine = new_connector.create_db_connector()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:

        while not stop_event.is_set():

            random_row_number = random_number_generator.randint(0, 11000)

            with engine.connect() as connection:
                dict_for_json = data_sender.produce_data_dict_for_request_payload(connection, random_row_number)

            pool.submit(data_sender.post_record_to_stream_layer, dict_for_json)
            pool.submit(data_sender.post_record_to_batch_layer, dict_for_json)

            sleep(random_number_generator.randrange(0, 2))

if __name__ == "__main__":

    stop_event = Event()

    shared_random_number_generator = random.Random(100)

    pin_data = DataSender("0a0223c10829.pin", "streaming-0a0223c10829-pin", "pinterest_data")
    geo_data = DataSender("0a0223c10829.geo", "streaming-0a0223c10829-geo", "geolocation_data", "timestamp")
    user_data = DataSender("0a0223c10829.user", "streaming-0a0223c10829-user", "user_data", "date_joined")

    data_senders = [pin_data, geo_data, user_data]
    processes = []

    for data_sender in data_senders:
        process = Process(target=worker, args=(data_sender, stop_event, shared_random_number_generator))
        process.start()
        processes.append(process)

    input("Data being sent... Hit enter to stop sending data to the pipeline...")

    stop_event.set()

    for process in processes:
        process.join()

    print("Data sending terminated.")