import boto3
from botocore.exceptions import ClientError

class DynamoDBManager:
    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def eliminar_tabla_si_existe(self, table_name):
        try:
            table = self.dynamodb.Table(table_name)
            table.load()  # Verifica si la tabla existe
            print(f"Eliminando tabla: {table_name}...")
            table.delete()
            table.wait_until_not_exists()
            print(f"Tabla {table_name} eliminada exitosamente.")
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            print(f"La tabla {table_name} no existe.")
    
    def eliminar_tablas(self, tablas):
        for tabla in tablas:
            self.eliminar_tabla_si_existe(tabla)