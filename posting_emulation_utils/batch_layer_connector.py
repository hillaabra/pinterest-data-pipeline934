import json
import requests

from .data_generator import DataGenerator


class BatchLayerConnector(DataGenerator):

    def __init__(self, topic_name, source_table_name, datetime_column_name = None):
        self.topic_name = topic_name
        self.api_batch_request_url = self._set_batch_request_url()
        super().__init__(source_table_name, datetime_column_name)

    def _set_batch_request_url(self):
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/topics/{self.topic_name}"
        return request_url

    def _post_record_to_batch_layer(self, dict_for_json):
        payload = json.dumps({
                "records": [
                    {
                        "value": dict_for_json
                    }
                ]
            })

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.post(url=self.api_batch_request_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f"Record successfully sent to topic {self.topic_name}")
        else:
            print(f"response.status_code for topic {self.topic_name}: {response.status_code}")
        # if response.status_code != 200:
        #     print(f"response.status_code for topic {self.topic_name}: {response.status_code}")
