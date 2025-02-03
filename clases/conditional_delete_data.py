import boto3
from botocore.exceptions import ClientError
import logging
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

class ConditionalDeleteRecords:
    """
    Encapsula una tabla de Amazon DynamoDB para manejar la eliminación condicional de registros.
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

    def conditional_delete_record(self, key, condition_expression):
        """
        Elimina un registro de la tabla solo si se cumple la condición definida.

        :param key: Diccionario que representa la clave del registro a eliminar.  
                    Ejemplo: {"client_id": 2, "client_name": "Ana López"}
        :param condition_expression: Expresión condicional para que se realice la eliminación.
                    Ejemplo: Attr("client_status").eq("Inactivo")
        :return: La respuesta de la operación delete_item.
        """
        try:
            response = self.table.delete_item(
                Key=key,
                ConditionExpression=condition_expression,
                ReturnValues="ALL_OLD"
            )
            return response
        except ClientError as err:
            logger.error(
                "No se pudo eliminar condicionalmente el registro %s de la tabla %s. Error: %s: %s",
                key,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
