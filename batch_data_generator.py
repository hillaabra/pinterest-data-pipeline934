import requests
import json
import yaml

from sqlalchemy import text

class BatchDataGenerator:

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