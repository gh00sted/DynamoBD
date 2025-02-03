import boto3
from botocore.exceptions import ClientError

class InsertData:
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource

    def insert_data(self, table_name, items):
        table = self.dyn_resource.Table(table_name)
        for item in items:
            try:
                table.put_item(Item=item)
                print(f"Registro insertado en {table_name}: {item}")
            except ClientError as err:
                print(f"Error al insertar en {table_name}: {err.response['Error']['Message']}")
