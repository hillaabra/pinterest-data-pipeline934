import json
import requests

from .data_generator import DataGenerator


class BatchLayerConnector(DataGenerator):
    """
    Class inheriting from DataGenerator. Defines the attributes and methods for
    sending data into the Batch Layer of the pipeline through the Kafka-integrated
    REST-proxy resource of the API, to be inherited by the child class DataSender.

    Parameters:
    ----------
    topic_name: str
        The name of the Kafka topic to send data through (consistent with the Kafka (EC2) Client).

    source_table_name: str
        The name of the table in the remote database of historic data, from which to extract random rows.

    datetime_column_name: None | str
        Default value: None. If exists, the name of the column in the source table
        that has a datetime-type value. (There is only 1 such column, if any.)

    Attributes:
    ----------
    topic_name: str
        The name assigned to the Kafka topic for this dataset for data ingestion through the REST
        proxy resource of the API.

    api_batch_request_url: str
        The URL for the API requests to the REST-proxy resource.

    source_table_name: str
        Inherited from the DataGenerator parent class; the name of the table in the remote
        database from which the historic data is being retrieved randomly to emulate user data generation.

    datetime_column_name: None | str
        Inherited from the DataGenerator parent class; if exists, the name of the column
        in the source table that has a datetime-type value.
    """
    def __init__(self, topic_name: str, source_table_name: str, datetime_column_name: str = None) -> None:
        """
        See help(BatchLayerConnector) for an accurate signature.
        """
        self.topic_name = topic_name
        self.api_batch_request_url = self._set_batch_request_url()
        super().__init__(source_table_name, datetime_column_name)

    def _set_batch_request_url(self) -> str:
        """
        Protected; method used by the class constructor method to set the object's
        api_batch_request_url attribute.

        Returns:
        -------
        str: The URL for the API requests to the REST proxy resource for data ingestion to
        the Batch Layer of the pipeline.
        """
        invoke_url = self._get_api_invoke_url()
        request_url = f"{invoke_url}/topics/{self.topic_name}"
        return request_url

    def post_record_to_batch_layer(self, dict_for_json: dict) -> None:
        """
        Method used to send a POST request to the API's REST-proxy resource to submit
        a record of data into the Batch Layer of the pipeline. The REST-proxy resource
        is integrated with AWS MSK Connect which will direct the record into its relevant
        topic folder within AWS S3.

        Argument:
        --------
        dict_for_json: dict
            The data extracted from the remote database ready for serialisation to JSON
            (all values are string or numeric).
        """
        # The payload is in the JSON-serialisation format supported by Confluent REST Proxy
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
            print(f"Record successfully sent to topic {self.topic_name}. \
                  Hit ENTER to stop sending data to the pipeline...")
        else:
            print(f"response.status_code for topic {self.topic_name}: {response.status_code}. \
                  Hit ENTER to stop sending data to the pipeline...")