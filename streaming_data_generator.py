import json
import requests

from data_generator import DataGenerator


class StreamingDataGenerator(DataGenerator):

    def __init__(self, stream_name, source_table_name, datetime_column_name = None):
        self.stream_name = stream_name
        super().__init__(source_table_name, datetime_column_name)

    def _set_request_url(self):
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/streams/{self.stream_name}/record"
        return request_url

    def _send_record_to_stream(self, dict_for_json):

        payload = json.dumps({
            "StreamName": "{self.stream_name}",
            "Data": dict_for_json,
            "PartitionKey": "partition-1"
            })

        headers = {'Content-Type': 'application/json'}

        response = requests.put(url=self.api_request_url, headers=headers, data=payload)
        return response.status_code

    def send_random_record_to_stream(self, connection, random_row_number):
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)
        http_response_status_code = self._send_record_to_stream(dict_for_json)
        return http_response_status_code
