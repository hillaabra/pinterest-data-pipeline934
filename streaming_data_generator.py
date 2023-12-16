import json
import requests

from data_generator import DataGenerator


class StreamingDataGenerator(DataGenerator):

    def __init__(self, stream_name, source_table_name, datetime_column_name = None):
        self.stream_name = stream_name
        super().__init__(source_table_name, datetime_column_name)

    def _get_invoke_url(self):
        dict_from_user_cred = self._load_dict_from_yaml("user_config.yaml")
        invoke_url = f"{dict_from_user_cred['api_gateway_streaming_invoke_url']}/streams/{self.stream_name}/record"
        return invoke_url

    def _send_record_to_stream(self, dict_for_json):

        payload = json.dumps({
            "StreamName": "{self.stream_name}",
            "Data": dict_for_json,
            "PartitionKey": "partition-1"
            })

        headers = {'Content-Type': 'application/json'}

        response = requests.put(url=self.invoke_url, headers=headers, data=payload)
        # TODO: Error handling
        print(f"response.status_code for stream {self.stream_name}: ", response.status_code)

    def send_random_record_to_stream(self, connection, random_row_number):
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)
        self._send_record_to_stream(dict_for_json)
