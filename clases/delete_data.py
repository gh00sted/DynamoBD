import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class DeleteRecords:
    """
    Encapsula una tabla de Amazon DynamoDB para manejar la eliminación de registros.
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: Un recurso Boto3 de DynamoDB.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def set_table(self, table_name):
        """
        Configura la tabla en la que se trabajará.
        
        :param table_name: Nombre de la tabla en DynamoDB.
        """
        self.table = self.dyn_resource.Table(table_name)

    def delete_record(self, key):
        """
        Elimina un registro de la tabla sin modificar la clave.
        
        :param key: Diccionario que representa la clave del registro a eliminar. Ejemplo:
                    {"client_id": 1, "client_name": "Juan Pérez"}
        :return: La respuesta de la operación delete_item.
        """
        try:
            response = self.table.delete_item(Key=key)
        except ClientError as err:
            logger.error(
                "No se pudo eliminar el registro %s de la tabla %s. Error: %s: %s",
                key,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response
