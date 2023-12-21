from multiprocessing import Process

from .batch_layer_connector import BatchLayerConnector
from .stream_layer_connector import StreamLayerConnector


class MultiprocessDataSender(BatchLayerConnector, StreamLayerConnector):

    def __init__(self, topic_name, stream_name, source_table_name, datetime_column_name = None):
        BatchLayerConnector.__init__(self, topic_name, source_table_name, datetime_column_name)
        StreamLayerConnector.__init__(self, stream_name, source_table_name, datetime_column_name)

    def post_random_record_to_batch_and_stream_layers(self, connection, random_row_number):

        selected_row = self._extract_random_record_from_aws_db(connection, random_row_number)
        dict_for_json = self._convert_sql_result_to_dict(selected_row)

        procs = []
        proc1 = Process(target=self._post_record_to_batch_layer, args=(dict_for_json,))
        procs.append(proc1)
        proc2 = Process(target=self._post_record_to_stream_layer, args=(dict_for_json,))
        procs.append(proc2)

        for proc in procs:
            proc.start()