from abc import ABC
import yaml

from sqlalchemy import engine, text


class DataGenerator(ABC):
    """
    Abstract Base Class which defines methods used by the BatchLayerConnector
    and StreamLayerConnector child classes and the DataSender grandchild class
    to emulate user data generation by extracting data from the specified table
    in a remote SQL database.

    Parameters:
    ----------
    source_table_name: str
        The name of the table in the remote database of historic data, from which to extract random rows.

    datetime_column_name: str
        Default value: None. If existing, the name of the column in the source table
        that has a datetime-type value. (There is only 1 such column, if any.)

    Attributes:
    ----------
    source_table_name: str
        The name of the table in the remote database from which the historic data is being retrieved
        randomly to emulate user data generation.

    datetime_column_name: None | str
        If exists, the name of the column within the specified table that has a datetime type value.
    """

    def __init__(self, source_table_name: str, datetime_column_name: str = None) -> None:
        '''
        See help(DataGenerator) for an accurate signature.
        '''
        self.source_table_name = source_table_name
        self.datetime_column_name = datetime_column_name

    @staticmethod
    def _load_dict_from_yaml(yaml_pathway: str) -> dict:
        '''
        Protected; static method used internally which loads the contents of a
        YAML file into a dictionary.

        Argument:
        --------
        yaml_pathway: str
            The filepath to the YAML file.

        Returns:
        -------
        dict: the contents of the YAML file
        '''
        with open(yaml_pathway, 'r') as stream:
            dict_from_yaml = yaml.safe_load(stream)
        return dict_from_yaml

    def _get_api_invoke_url(self) -> str:
        '''
        Protected; method used internally to retrieve the invoke URL from "posting_emulation_utils/api_gateway_config.yaml"
        of the API deployed on API Gateway for data ingestion into the pipeline.

        Returns:
        -------
        str: the invoke URL of the API
        '''
        dict_from_api_cred = self._load_dict_from_yaml("posting_emulation_utils/api_gateway_config.yaml")
        invoke_url = dict_from_api_cred["api_gateway_invoke_url"]
        return invoke_url

    def _make_record_dict_json_friendly(self, dict_retrieved_record: dict) -> dict:
        '''
        Protected; method used internally to cast to string (in ISO format) the datetime-type
        column value of the dataset retrieved from the remote database.

        Argument:
        --------
        dict_retrieved_record: dict
            The dictionary storing the data from the retrieved record.

        Returns:
        -------
        dict: The data from the retrieved record with all values as string or numeric types.
        '''
        dict_retrieved_record[self.datetime_column_name] = dict_retrieved_record[self.datetime_column_name].strftime("%Y:%m:%d %H:%M:%S")
        return dict_retrieved_record

    def _extract_random_record_from_aws_db(self, connection: engine.Connection, random_row_number: int) -> engine.CursorResult:
        '''
        Protected; method used internally to extract a random record from the RDS database on AWS,
        from the dataset specified in the source_table_name attribute.

        Arguments:
        ---------
        connection: engine.Connection
            A sqlalchemy.engine.Connection object, initialised in the user_posting_emulation script's
            run_infinite_post_loop function for connection to the AWS RDS database.

        random_row_number: int
            An integer value representing the row/index number of the record to be extracted from the
            RDS database on AWS.

        Returns:
        -------
        engine.CursorResult
            A sqlalchemy.engine.CursorResult object pointing at the result of the SQL query performed
            on the remote database.
        '''
        query_string = text(f"SELECT * FROM {self.source_table_name} LIMIT {random_row_number}, 1")
        selected_row = connection.execute(query_string)
        return selected_row

    def _convert_sql_result_to_dict(self, selected_row: engine.CursorResult) -> dict:
        '''
        Protected; method used internally to convert the result of the SQL query performed on the remote
        database into a dictionary object. If the dataset has a datetime-type column value, the
        method invokes the method to cast that value to string.

        Argument:
        --------
        selected_row: engine.CursorResult
            The sqlalchemy.engine.CursorResult object pointing at the result of the query retrieving
            a row of data from the remote database.

        Returns:
        -------
        dict: The data retrieved from the remote database, now compatible with the API payload requirements.
        '''
        for row in selected_row:
            dict_for_json = dict(row._mapping)
        if self.datetime_column_name is not None:
            dict_for_json = self._make_record_dict_json_friendly(dict_for_json)
        return dict_for_json