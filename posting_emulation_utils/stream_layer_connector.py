import json
import requests

from .data_generator import DataGenerator


class StreamLayerConnector(DataGenerator):

    def __init__(self, stream_name, source_table_name, datetime_column_name = None):
        self.stream_name = stream_name
        self.api_stream_request_url = self._set_stream_request_url()
        super().__init__(source_table_name, datetime_column_name)

    def _set_stream_request_url(self):
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/streams/{self.stream_name}/record"
        return request_url

    def _post_record_to_stream_layer(self, dict_for_json):

        payload = json.dumps({
            "StreamName": "{self.stream_name}",
            "Data": dict_for_json,
            "PartitionKey": "partition-1"
            })

        headers = {'Content-Type': 'application/json'}

        response = requests.put(url=self.api_stream_request_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f"Record successfully sent to stream {self.stream_name}")
        else:
            print(f"response.status_code for stream {self.stream_name}: {response.status_code}")
        # if response.status_code != 200:
        #     print(f"response.status_code for stream {self.stream_name}: {response.status_code}")
