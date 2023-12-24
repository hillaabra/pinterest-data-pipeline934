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
        The name of the Kafka topic data to send data through. This should match
        the naming of the Kafka topic created for this dataset in the Kafka (EC2) Client.

    source_table_name: str
        The name of the table in the pre-existing SQL database of data being extracted from.

     datetime_column_name: str
        Default value: None. If existing, the name of the column within the specified table
        that has a datetime type value. (There is only one such column in two out of the
        three datasets being extracted from.)

    Attributes:
    ----------
    topic_name: str
        The name assigned to the Kafka topic for this dataset for data ingestion through the REST
        proxy resource of the API.

    api_batch_request_url: str
        The URL for the API requests to the REST proxy resource; set on initiation by invoking the
        internal method _set_batch_request_url().

    source_table_name: str
        Inherited from the DataSender parent class; the name of the table in the pre-existing
        SQL database of data being extracted from.

    datetime_column_name: str
        Inherited from the DataSender parent class; initialised as None by default; if exists,
        the name of the column within the specified table that has a datetime type value.
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
        Method used to send a JSON-serialised post request to the API's REST proxy
        resource to submit a record of data to the Batch Layer of the pipeline.
        The REST proxy resource is integrated with AWS MSK Connect which will direct
        the record into its relevant topic folder within the dedicated S3 bucket.

        Argument:
        --------
        dict_for_json: dict
            Dictionary object containing the data extracted from the remote database
            ready for serialisation to JSON.
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
        # if response.status_code == 200:
        #     print(f"Record successfully sent to topic {self.topic_name}")
        # else:
        #     print(f"response.status_code for topic {self.topic_name}: {response.status_code}")
        if response.status_code != 200:
            print(f"response.status_code for topic {self.topic_name}: {response.status_code}")
