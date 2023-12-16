from abc import abstractmethod, ABC
import yaml

from sqlalchemy import text

class DataGenerator(ABC):

    def __init__(self, source_table_name, datetime_column_name = None):
        self.source_table_name = source_table_name
        self.invoke_url = self._get_invoke_url()
        self.datetime_column_name = datetime_column_name

    @staticmethod
    def _load_dict_from_yaml(yaml_pathway):
        with open(yaml_pathway, 'r') as stream:
            dict = yaml.safe_load(stream)
        return dict

    @abstractmethod
    def _get_invoke_url(self):
        pass

    def _make_record_dict_json_friendly(self, dict):
        dict[self.datetime_column_name] = dict[self.datetime_column_name].strftime("%Y:%m:%d %H:%M:%S")
        return dict

    def _extract_random_record_from_aws_db(self, connection, random_row_number):
        query_string = text(f"SELECT * FROM {self.source_table_name} LIMIT {random_row_number}, 1")
        selected_row = connection.execute(query_string)
        return selected_row

    def _convert_sql_result_to_dict(self, selected_row):
        for row in selected_row:
            dict_for_json = dict(row._mapping)
        if self.datetime_column_name is not None:
            dict_for_json = self._make_record_dict_json_friendly(dict_for_json)
        return dict_for_json
