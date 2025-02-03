import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class ScanRecords:
    """
    Encapsula una tabla de Amazon DynamoDB para obtener todos los registros mediante un scan.
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: Un recurso Boto3 de DynamoDB.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def set_table(self, table_name):
        """
        Configura la tabla sobre la que se trabajará.
        
        :param table_name: Nombre de la tabla en DynamoDB.
        """
        self.table = self.dyn_resource.Table(table_name)

    def scan_table(self):
        """
        Obtiene todos los registros de la tabla mediante un scan.
        
        :return: Una lista con todos los registros de la tabla.
        """
        if not self.table:
            logger.error("No se ha configurado ninguna tabla. Llama a set_table primero.")
            return []

        try:
            response = self.table.scan()
            items = response.get("Items", [])
            # Si la respuesta está paginada, seguimos escaneando
            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))
            return items
        except ClientError as err:
            logger.error(
                "No se pudieron obtener los registros de la tabla %s. Error: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return []
