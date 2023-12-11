import requests
from time import sleep
import random
from multiprocessing import Process
import yaml
import boto3
import json
import sqlalchemy
from sqlalchemy import text


# %%
class AWSDBConnector:

    def __init__(self, credentials_yaml):

        with open(credentials_yaml, 'r') as stream:
            dict_db_creds = yaml.safe_load(stream)

        self.HOST = dict_db_creds["HOST"]
        self.USER = dict_db_creds["USER"]
        self.PASSWORD = dict_db_creds["PASSWORD"]
        self.DATABASE = dict_db_creds["DATABASE"]
        self.PORT = dict_db_creds["PORT"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


# %%
class DataSender:

    def __init__(self, topic_name, source_table_name, datetime_column_name = None):
        self.topic_name = topic_name
        self.source_table_name = source_table_name
        self.invoke_url = self._get_invoke_url()
        self.datetime_column_name = datetime_column_name

    @staticmethod
    def _load_dict_from_yaml(yaml_pathway):
        with open(yaml_pathway, 'r') as stream:
            dict = yaml.safe_load(stream)
        return dict

    def _get_invoke_url(self):
        dict_from_user_cred = self._load_dict_from_yaml("user_config.yaml")
        invoke_url = f"{dict_from_user_cred['api_gateway_invoke_url']}/topics/{self.topic_name}"
        return invoke_url

    def _make_record_dict_json_friendly(self, dict):
        dict[self.datetime_column_name] = dict[self.datetime_column_name].strftime("%Y:%m:%d %H:%M:%S")
        return dict

    def _extract_random_record_from_aws_db(self, connection, random_row_number):
        query_string = text(f"SELECT * FROM {self.source_table_name} LIMIT {random_row_number}, 1")
        selected_row = connection.execute(query_string)
        return selected_row

    def _convert_sql_result_to_dict(self, selected_row):
        for row in selected_row:
            dict_for_json = dict(row._mapping)
        if self.datetime_column_name is not None:
            dict_for_json = self._make_record_dict_json_friendly(dict_for_json)
        return dict_for_json

    def _send_record_to_topic(self, dict_for_json):
        payload = json.dumps({
                "records": [
                    {
                        "value": dict_for_json
                    }
                ]
            })

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.post(url=self.invoke_url, headers=headers, data=payload)
        # TODO: Error handling
        print(f"response.status_code for topic {self.topic_name}: ", response.status_code)

    def send_random_record_to_topic(self, connection, random_row_number):
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)
        self._send_record_to_topic(dict_for_json)


# %%
new_connector = AWSDBConnector("aws_db_config.yaml")
pin_data_poster = DataSender("0a0223c10829.pin", "pinterest_data")
geo_data_poster = DataSender("0a0223c10829.geo", "geolocation_data", "timestamp")
user_data_poster = DataSender("0a0223c10829.user", "user_data", "date_joined")

data_senders_by_topic = [pin_data_poster, geo_data_poster, user_data_poster]

random.seed(100)
# %%
def run_infinite_post_data_loop():
    engine = new_connector.create_db_connector()
    while True:
        sleep(random.randrange(0, 2))
        random_row_number = random.randint(0, 11000)
        with engine.connect() as connection:
            for data_sender in data_senders_by_topic:
                data_sender.send_random_record_to_topic(connection, random_row_number)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
