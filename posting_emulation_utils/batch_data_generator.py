import json
import requests

from .data_generator import DataGenerator


class BatchDataGenerator(DataGenerator):

    def __init__(self, topic_name, source_table_name, datetime_column_name = None):
        self.topic_name = topic_name
        super().__init__(source_table_name, datetime_column_name)

    def _set_request_url(self):
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/topics/{self.topic_name}"
        return request_url

    def _send_record_to_topic(self, dict_for_json):
        payload = json.dumps({
                "records": [
                    {
                        "value": dict_for_json
                    }
                ]
            })

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.post(url=self.api_request_url, headers=headers, data=payload)
        return response.status_code

    def send_random_record_to_topic(self, connection, random_row_number):
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)
        http_reponse_status_code = self._send_record_to_topic(dict_for_json)
        return http_reponse_status_code