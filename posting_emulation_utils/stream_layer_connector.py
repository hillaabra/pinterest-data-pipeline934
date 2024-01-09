import json
import requests

from .data_generator import DataGenerator


class StreamLayerConnector(DataGenerator):
    """
    Class inheriting from DataGenerator. Defines the attributes and methods for
    sending data into the Stream Layer of the pipeline through the Kinesis-integrated
    resource of the API, to be inherited by the child class DataSender.

    Parameters:
    ----------
    stream_name: str
        The name of the Kinesis data stream to send data to. This should match
        the naming of the data streams created for this dataset on the AWS Kinesis console.

    source_table_name: str
        The name of the table in the pre-existing SQL database of data being extracted from.

    datetime_column_name: str
        Default value: None. If existing, the name of the column within the specified table
        that has a datetime type value. (There is only one such column in two out of the
        three datasets being extracted from.)

    Attributes:
    ----------
    stream_name: str
        The name assigned to the Kinesis stream for this dataset for data ingestion through
        the Kinesis-integrated resource of the API.

    api_stream_request_url: str
        The URL for the API requests to the Kinesis-integrated resource; set on initiation
        by invoking the internal method _set_batch_request_url().

    source_table_name: str
        Inherited from the DataSender parent class; the name of the table in the pre-existing
        SQL database of data being extracted from.

    datetime_column_name: str
        Inherited from the DataSender parent class; initialised as None by default; if exists,
        the name of the column within the specified table that has a datetime type value.
    """
    def __init__(self, stream_name: str, source_table_name: str, datetime_column_name: str = None):
        """
        See help(StreamLayerConnector) for an accurate signature.
        """
        self.stream_name = stream_name
        self.api_stream_request_url = self._set_stream_request_url()
        super().__init__(source_table_name, datetime_column_name)

    def _set_stream_request_url(self) -> str:
        """
        Protected; method used by the class constructor method to set the object's
        api_stream_request_url attribute.

        Returns:
        -------
        str: The URL for the API requests to the Kinesis-integrated resource for data
        ingestion to the Stream Layer of the pipeline.
        """
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/streams/{self.stream_name}/record"
        return request_url

    def post_record_to_stream_layer(self, dict_for_json: dict) -> None:
        """
        Method used to send a JSON-serialised post request to the API's Kinesis-integrated
        resource to submit a record of data to the Stream Layer of the pipeline.
        As per the API method's integration request defined on API Gateway, the payload
        specifies the Kinesis stream to which the API should deliver the data
        (which the method sets here using the object's stream_name attribute).

        Argument:
        --------
        dict_for_json: dict
            Dictionary object containing the data extracted from the remote database
            ready for serialisation to JSON.
        """
        payload = json.dumps({
            "StreamName": "{self.stream_name}",
            "Data": dict_for_json,
            "PartitionKey": "partition-1"
            })

        headers = {'Content-Type': 'application/json'}

        response = requests.put(url=self.api_stream_request_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f"Record successfully sent to stream {self.stream_name}. Hit ENTER to stop sending data to the pipeline...")
        else:
            print(f"response.status_code for stream {self.stream_name}: {response.status_code}. Hit ENTER to stop sending data to the pipeline...")