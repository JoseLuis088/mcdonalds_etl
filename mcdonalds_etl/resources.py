from dotenv import dotenv_values
from dagster import ConfigurableResource

class SqlConnection(ConfigurableResource):
    server: str
    database: str
    username: str
    password: str
    driver: str

    def connection_string(self) -> str:
        return f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver={self.driver}&TrustServerCertificate=yes"

    def pyodbc_connection_string(self):
        return f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'

config = dotenv_values(".env")

database_resource = SqlConnection(
    server = config["SERVER_DB"],
    database = config["DATABASE_DB"],
    username = config["USERNAME_DB"],
    password = config["PASSWORD_DB"],
    driver = config["DRIVER"]
)