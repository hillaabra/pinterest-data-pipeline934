import yaml
import sqlalchemy

class AWSDBConnector:

    def __init__(self, credentials_yaml):

        with open(credentials_yaml, 'r') as stream:
            dict_db_creds = yaml.safe_load(stream)

        self.HOST = dict_db_creds["HOST"]
        self.USER = dict_db_creds["USER"]
        self.PASSWORD = dict_db_creds["PASSWORD"]
        self.DATABASE = dict_db_creds["DATABASE"]
        self.PORT = dict_db_creds["PORT"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
