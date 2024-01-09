import concurrent.futures
import random

from multiprocessing import Event, Process
from time import sleep

from posting_emulation_utils.aws_db_connector import AWSDBConnector
from posting_emulation_utils.data_sender import DataSender


new_connector = AWSDBConnector("posting_emulation_utils/aws_db_config.yaml")

def run_infinite_post_loop(data_sender: DataSender,
                            stop_event: Event,
                            random_number_generator: random.Random) -> None:
    """
    Function that sends a record at random, every 0-2 seconds, from the source table in the
    remote AWS database to both the Stream Layer and Batch Layer of the pipeline concurrently.
    The infinite loop is interrupted if a user input event is detected.

    Arguments:
    ---------
    data_sender: DataSender
        An instance of the DataSender class which defines which table the data is being extracted
        from and therefore which topic and stream the data is to be ingested through.

    stop_event: Event
        A multiprocessing.Event object which tells the infinite post loop to stop running if set.

    random_number_generator: random.Random
        A random number generator object initialised outside the function definition to ensure
        parallelism in the time delay counts and index values of the rows of data being randomly
        extracted and posted in the course of each dataset-specific process.
    """

    # establishes a SQLAlchemy engine object for connection to the RDS Database on AWS of pre-existing data
    engine = new_connector.create_db_connector()

    # creates a thread pool in order to send data to the batch and stream layers concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:

        while not stop_event.is_set():

            # the random_row_number selected on each iteration of the loop is consistent across
            # all processes (so each process extracts a row of data with N index - in total producing
            # 3 datapoints relating to the same user posting event)
            random_row_number = random_number_generator.randint(0, 11000)

            # extracts a row of data from the remote database, to emulate user posting data generation
            with engine.connect() as connection:
                dict_for_api_request = data_sender.produce_data_dict_for_request_payload(connection, random_row_number)

            # submits tasks to the thread pool executor
            pool.submit(data_sender.post_record_to_stream_layer, dict_for_api_request)
            pool.submit(data_sender.post_record_to_batch_layer, dict_for_api_request)

            # there is a 0 to 2 second delay after each data posting event, which is synchronised
            # between the processes thanks to the shared random number generator object parameter
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
        process = Process(target=run_infinite_post_loop, args=(data_sender, stop_event, shared_random_number_generator))
        process.start()
        processes.append(process)

    input("Data being sent... Hit enter to stop sending data to the pipeline...\n")

    stop_event.set()

    for process in processes:
        process.join()

    print("Data sending terminated.")