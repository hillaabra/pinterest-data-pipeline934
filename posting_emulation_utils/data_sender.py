from .batch_layer_connector import BatchLayerConnector
from .stream_layer_connector import StreamLayerConnector

import sqlalchemy

class DataSender(BatchLayerConnector, StreamLayerConnector):
    """
    Class inheriting from BatchLayerConnector and StreamLayerConnector classes.
    Defines method for providing data to the API requests, to be used in the
    user_posting_emulation script.

    Parameters:
    ----------
    topic_name: str
        The name of the Kafka topic to send data through. This should match
        the naming of the Kafka topic created for this dataset in the Kafka (EC2) Client.

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
    topic_name: str
        The name assigned to the Kafka topic for this dataset for data ingestion through the REST
        proxy resource of the API; assigned by the constructor of the BatchLayerConnector parent class.

    stream_name: str
        The name assigned to the Kinesis stream for this dataset for data ingestion through
        the Kinesis-integrated resource of the API; assigned by the constructor of the
        StreamLayerConnector parent class.

    api_batch_request_url: str
        The URL for the API requests to the REST proxy resource; set on initiation by the
        constructor of the BatchLayerConnector parent class.

    api_stream_request_url: str
        The URL for the API requests to the Kinesis-integrated resource; set on initiation by the
        constructor of the StreamLayerConnector parent class.

    source_table_name: str
        The name of the table in the pre-existing SQL database of data being extracted from;
        assigned by the parent class constructors.

    datetime_column_name: str
        Initialised as None by default; if exists, the name of the column within the
        specified table that has a datetime type value; assigned by the parent class constructors.

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
        Method that extracts the nth row from the dataset's table in the remote database, where
        n is the randomly selected number generated in the user_posting_emulation script by the
        run_post_infinite_data loop function. The method returns the SQL query result as a
        dictionary object ready for JSON-serialisation.

        Arguments:
        ---------
        connection: sqlalchemy.engine.Connection
            A sqlalchemy.engine.Connection object, initialised in the user_posting_emulation script's
            run_infinite_post_loop function for connection to the AWS RDS database.

        random_row_number: int
            An integer value representing the row/index number of the record to be extracted from the
            RDS database on AWS.

        Returns:
        -------
        dict: A dictionary object containing the data retrieved from the remote database ready for placement
        in the API payload.
        """
        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)

        return dict_for_json