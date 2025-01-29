import boto3
from botocore.exceptions import ClientError

#Creamos una clase para crear tablas con parametros
class DataTables:
    #Constructor de la clase
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    # Funcion para crear la tabla con parametros
    def create_table(self, table_name, partition_key, sort_key):
        try:
            #Funcion .create_table de dynamodb
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": partition_key, "KeyType": "HASH"},
                    {"AttributeName": sort_key, "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": partition_key, "AttributeType": "N"},
                    {"AttributeName": sort_key, "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            )
            self.table.wait_until_exists()
        except ClientError as err:
            print(f"Error al crear la tabla {table_name}: {err.response['Error']['Message']}")
            raise
        else:
            print(f"Tabla {table_name} creada con éxito!")
            return self.table