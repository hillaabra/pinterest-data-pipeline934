import json
import requests

from .data_generator import DataGenerator


class StreamLayerConnector(DataGenerator):
    """
    Class extending parent class DataGenerator. Defines the attributes and methods
    to be inherited by child class DataSender for sending data into the Stream Layer
    of the pipeline.

    Parameters:
    ----------
    stream_name: str
        The name of the AWS Kinesis data stream to send data to.

    source_table_name: str
        The name of the table in the remote database of historic data, from which to extract random rows.

    datetime_column_name: None | str
        Default value: None. If exists, the name of the column in the source table
        that has a datetime-type value. (There is only 1 such column, if any.)

    Attributes:
    ----------
    stream_name: str
        The name assigned to the Kinesis stream for this dataset for data ingestion through
        the Kinesis-integrated resource of the API.

    api_stream_request_url: str
        The URL for the API requests to the Kinesis-integrated resource.

    source_table_name: str
        Inherited from DataGenerator parent class; the name of the table in the remote
        database from which the historic data is being retrieved randomly to emulate user data generation

    datetime_column_name: None | str
        Inherited from the DataGenerator parent class; if exists, the name of the column
        in the source table that has a datetime-type value.
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
        Method used to send a POST request to the API's Kinesis-integrated
        resource to submit a record of data into the Stream Layer of the pipeline.
        The payload specifies the Kinesis stream to which the API should deliver the data.

        Argument:
        --------
        dict_for_json: dict
            The data extracted from the remote database ready for serialisation to JSON
            (all values are string or numeric).
        """
        payload = json.dumps({
            "StreamName": "{self.stream_name}",
            "Data": dict_for_json,
            "PartitionKey": "partition-1"
            })

        headers = {'Content-Type': 'application/json'}

        response = requests.put(url=self.api_stream_request_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f"Record successfully sent to stream {self.stream_name}. \
                  Hit ENTER to stop sending data to the pipeline...")
        else:
            print(f"response.status_code for stream {self.stream_name}: {response.status_code}. \
                  Hit ENTER to stop sending data to the pipeline...")