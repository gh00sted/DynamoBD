import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class DataRecords:
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: Un recurso Boto3 de DynamoDB.
        """
        self.dyn_resource = dyn_resource
        self.table = None
        self.partition_key = None
        self.sort_key = None

    def set_table(self, table_name, partition_key, sort_key=None):
        """
        Configura la tabla con la que se trabajará.

        :param table_name: Nombre de la tabla en DynamoDB.
        :param partition_key: Nombre del atributo de clave primaria.
        :param sort_key: (Opcional) Nombre del atributo de clave de ordenación.
        """
        self.table = self.dyn_resource.Table(table_name)
        self.partition_key = partition_key
        self.sort_key = sort_key  # Puede ser None si la tabla no tiene sort_key

    def get_record(self, partition_key_value, sort_key_value=None):
        """
        Obtiene un registro de la tabla especificada en DynamoDB.

        :param partition_key_value: Valor del atributo de clave primaria.
        :param sort_key_value: (Opcional) Valor del atributo de clave de ordenación.
        :return: El registro obtenido de la tabla o None si no se encuentra.
        """
        if not self.table:
            logger.error("No se ha configurado ninguna tabla. Llama a set_table primero.")
            return None

        if self.sort_key and sort_key_value is None:
            logger.error(f"La tabla '{self.table.name}' requiere una clave de ordenación '{self.sort_key}', pero no se proporcionó.")
            return None

        key = {self.partition_key: partition_key_value}
        if self.sort_key:
            key[self.sort_key] = sort_key_value

        try:
            response = self.table.get_item(Key=key)
        except ClientError as err:
            logger.error(
                "No se pudo obtener el registro %s de la tabla %s. Error: %s: %s",
                key,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return None
        else:
            return response.get("Item", None)
