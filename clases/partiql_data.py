# clases/partiql_data.py
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class PartiQLStatements:
    """
    Encapsula el uso de PartiQL para interactuar con las tablas de DynamoDB.
    """

    def __init__(self, ddb_client):
        """
        :param ddb_client: Un cliente Boto3 de DynamoDB.
        """
        self.client = ddb_client

    def execute_partiql(self, statement):
        """
        Ejecuta una instrucción PartiQL y devuelve los elementos obtenidos.
        
        :param statement: La instrucción PartiQL a ejecutar.
        :return: Lista de elementos (Items) o una lista vacía en caso de error.
        """
        try:
            response = self.client.execute_statement(Statement=statement)
            return response.get("Items", [])
        except ClientError as err:
            logger.error(
                "Error ejecutando PartiQL: %s. Error: %s: %s",
                statement,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return []
