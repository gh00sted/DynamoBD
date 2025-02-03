# clases/filter_data.py
import boto3
from botocore.exceptions import ClientError
import logging
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

class FilterRecords:
    """
    Encapsula una tabla de Amazon DynamoDB para obtener registros filtrados
    mediante un scan con FilterExpression.
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

    def filter_records(self, filter_expression):
        """
        Obtiene los registros de la tabla que cumplen con la expresión de filtrado.

        :param filter_expression: Una expresión de filtrado (por ejemplo, usando Attr).
        :return: Una lista con todos los registros que cumplen el filtro.
        """
        if not self.table:
            logger.error("No se ha configurado ninguna tabla. Llama a set_table primero.")
            return []

        try:
            response = self.table.scan(FilterExpression=filter_expression)
            items = response.get("Items", [])
            # Manejo de paginacion, en caso de que existan muchos registros.
            while "LastEvaluatedKey" in response:
                response = self.table.scan(
                    FilterExpression=filter_expression,
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
            return items
        except ClientError as err:
            logger.error(
                "No se pudieron obtener registros filtrados de la tabla %s. Error: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return []
