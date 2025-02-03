# clases/backup_data.py
import time
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class BackupRecords:
    """
    Encapsula la funcionalidad para crear un backup de una tabla de DynamoDB.
    """

    def __init__(self, ddb_client):
        """
        :param ddb_client: Un cliente de bajo nivel de DynamoDB (boto3.client('dynamodb')).
        """
        self.client = ddb_client

    def create_backup(self, table_name, backup_name=None):
        """
        Crea un backup de la tabla especificada.
        
        :param table_name: Nombre de la tabla a respaldar.
        :param backup_name: (Opcional) Nombre del backup. Si no se proporciona, se genera uno
                            concatenando el nombre de la tabla y un timestamp.
        :return: Los detalles del backup creado.
        """
        if not backup_name:
            # Genera un nombre de backup único usando el nombre de la tabla y el timestamp actual.
            backup_name = f"{table_name}_backup_{int(time.time())}"
        try:
            response = self.client.create_backup(
                TableName=table_name,
                BackupName=backup_name
            )
            return response["BackupDetails"]
        except ClientError as err:
            logger.error(
                "Error al crear backup para la tabla %s: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
