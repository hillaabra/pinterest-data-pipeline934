from abc import ABC
import yaml

from sqlalchemy import engine, text

class DataGenerator(ABC):
    """
    Abstract Base Class at the top of the class hierarchy, which defines
    the methods to be used by the child class to extract data at random
    from the specified table in a remote SQL database in order to emulate
    user posting data being generated.

    Parameters:
    ----------
    source_table_name: str
        The name of the table in the pre-existing SQL database of data being extracted from.
    datetime_column_name: str
        Default value: None. If existing, the name of the column within the specified table
        that has a datetime type value. (There is only one such column in two out of the
        three datasets being extracted from.)

    Attributes:
    ----------
    source_table_name: str
        The name of the table in the pre-existing SQL database of data being extracted from.
    datetime_column_name: str
        Initialised as None by default; if exists, the name of the column within the specified table
        that has a datetime type value.
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
        dict: dictionary object containing the contents of the YAML file.
        '''
        with open(yaml_pathway, 'r') as stream:
            dict = yaml.safe_load(stream)
        return dict

    def _get_api_invoke_url(self) -> str:
        '''
        Protected; method used internally to retrieve the invoke URL of the API deployed on API Gateway
        for data ingestion into the pipeline from the api_gateway_config.yaml file stored in the
        posting_emulation_utils directory of the repository.

        Returns:
        -------
        str: the invoke URL of the API on API Gateway
        '''
        dict_from_api_cred = self._load_dict_from_yaml("posting_emulation_utils/api_gateway_config.yaml")
        invoke_url = dict_from_api_cred["api_gateway_invoke_url"]
        return invoke_url

    def _make_record_dict_json_friendly(self, dict: dict) -> dict:
        '''
        Protected; method used internally to cast to string (in ISO format) the datetime-type column value
        in the dictionary storing the data from the record retrieved from the RDS database on AWS. It makes use
        of the datetime_column_name attribute the instance of the class has been initialised with.

        Argument:
        --------
        dict: dict
            The dictionary storing the data from the record retrieved from the RDS database on AWS.

        Returns:
        -------
        dict: The dictionary storing the data from the record retrieved from the RDS database on AWS,
        with all values as numeric or string types, now safe for loading into JSON.
        '''
        dict[self.datetime_column_name] = dict[self.datetime_column_name].strftime("%Y:%m:%d %H:%M:%S")
        return dict

    def _extract_random_record_from_aws_db(self, connection: engine.Connection, random_row_number: int) -> engine.CursorResult:
        '''
        Protected; method used internally to extract a random record from the RDS database on AWS,
        from the dataset specified in the source_table_name object attribute on initialisation.

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
            on the remote database which is returning the nth of the table, where n is the random number
            passed to this method.
        '''
        query_string = text(f"SELECT * FROM {self.source_table_name} LIMIT {random_row_number}, 1")
        selected_row = connection.execute(query_string)
        return selected_row

    def _convert_sql_result_to_dict(self, selected_row: engine.CursorResult) -> dict:
        '''
        Protected; method used internally to convert the result of the SQL query performed on the remote
        database (the single row of data from the specified dataset) into a dictionary object. If the
        datetime_column_name of the object is not None, the method passes the dictionary to the
        _make_record_dict_json_friendly() method for the datetime_column_value to be cast to string format.

        Argument:
        --------
        selected_row: engine.CursorResult
            The sqlalchemy.engine.CursorResult object pointing at the result of the SQL query performed
            on the remote database to extract the row of data from the randomly selected row.

        Returns:
        -------
        dict: A dictionary object containing the data retrieved from the remote database ready for placement
            in the API payload.
        '''
        for row in selected_row:
            dict_for_json = dict(row._mapping)
        if self.datetime_column_name is not None:
            dict_for_json = self._make_record_dict_json_friendly(dict_for_json)
        return dict_for_json