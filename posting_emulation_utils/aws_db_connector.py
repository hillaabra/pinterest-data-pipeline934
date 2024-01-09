import yaml
import sqlalchemy


class AWSDBConnector:
    """
    Class providing connection to a MySQL database using SQLAlchemy and PyMySQL.
    Used for this pipeline to provide connection to an RDS database on AWS storing
    historic Pinterest user event data.

    Parameter:
    --------
    credentials_yaml: str
        The filepath to the YAML file containing the required SQLAlchemy engine
        creation settings.

    Attributes:
    ----------
    HOST: str
        The "HOST" setting loaded from credentials YAML file

    USER: str
        The "USER" setting loaded from the credentials YAML file

    PASSWORD: str
        The "PASSWORD" setting loaded from the credentials YAML file

    DATABASE: str
        The "DATABASE" setting loaded from the credentials YAML file

    PORT: int
        The "PORT" setting loaded from the credentials YAML file
    """
    def __init__(self, credentials_yaml: str) -> None:
        """
        See help(AWSDBConnector) for an accurate signature.
        """
        with open(credentials_yaml, 'r') as stream:
            dict_db_creds = yaml.safe_load(stream)

        self.HOST = dict_db_creds["HOST"]
        self.USER = dict_db_creds["USER"]
        self.PASSWORD = dict_db_creds["PASSWORD"]
        self.DATABASE = dict_db_creds["DATABASE"]
        self.PORT = dict_db_creds["PORT"]

    def create_db_connector(self) -> sqlalchemy.engine:
        """
        Method to create a SQLAlchemy Engine object for connecting to the remote database.

        Returns:
        -------
        sqlalchemy.engine: a SQLAlchemy Engine object for connection to the remote database.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine