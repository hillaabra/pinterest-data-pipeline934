from .batch_layer_connector import BatchLayerConnector
from .stream_layer_connector import StreamLayerConnector

import sqlalchemy


class DataSender(BatchLayerConnector, StreamLayerConnector):
    """
    Class inheriting from BatchLayerConnector and StreamLayerConnector classes.
    Defines method for providing data to the API requests.

    Parameters:
    ----------

    topic_name: str
        The name of the Kafka topic to send data through (consistent with the Kafka (EC2) Client).

    stream_name: str
        The name of the AWS Kinesis data stream to send data to.

    source_table_name: str
        The name of the table in the remote database of historic data, from which to extract random rows.

    datetime_column_name: None | str
        Default value: None. If exists, the name of the column in the source table that has a
        datetime-type value. (There is only 1 such column, if any.)

    Attributes:
    ----------
    topic_name: str
        The name assigned to the Kafka topic for this dataset for data ingestion through the REST
        proxy resource of the API.

    stream_name: str
        The name assigned to the Kinesis stream for this dataset for data ingestion through
        the Kinesis-integrated resource of the API.

    api_batch_request_url: str
        The URL for the API requests to the REST proxy resource.

    api_stream_request_url: str
        The URL for the API requests to the Kinesis-integrated resource.

    source_table_name: str
        The name of the table in the remote database from which the historic data is being retrieved
        randomly to emulate user data generation.

    datetime_column_name: None | str
        If exists, the name of the column in the source table that has a datetime-type value.
    """
    def __init__(self,
                 topic_name: str,
                 stream_name: str,
                 source_table_name: str,
                 datetime_column_name: str = None) -> None:
        """
        See help(DataSender) for an accurate signature.
        """
        BatchLayerConnector.__init__(self, topic_name, source_table_name, datetime_column_name)
        StreamLayerConnector.__init__(self, stream_name, source_table_name, datetime_column_name)

    def produce_data_dict_for_request_payload(self,
                                              connection: sqlalchemy.engine.Connection,
                                              random_row_number: int) -> dict:
        """
        Method that extracts a row from the dataset's source table in the remote database.

        Arguments:
        ---------
        connection: sqlalchemy.engine.Connection
            A sqlalchemy.engine.Connection object, initialised in the user_posting_emulation script's
            run_infinite_post_loop function for connection to the AWS RDS database.

        random_row_number: int
            An integer value representing the row/index number of the record to be extracted.

        Returns:
        -------
        dict: The data retrieved from the remote database ready for placement in the API payload.
        """
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)

        return dict_for_json