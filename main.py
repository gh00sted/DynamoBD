import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

load_dotenv()

# Crear cliente de DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("REGION_NAME")
)

# 1-Crear al menos 3 tablas con dos atributos cada una ----------------------------------------------------------------------------

# Importamos la clase para crear tablas con 2 atributos
from clases.data_tables import DataTables

# Instanciamos 
data_tables = DataTables(dynamodb)

# Creamos las 3 tablas con parametros a gusto
table1 = data_tables.create_table('TablaClientes', 'client_id', 'client_name')
table2 = data_tables.create_table('TablaProductos', 'product_id', 'product_name')
table3 = data_tables.create_table('TablaEmpleados', 'employee_id', 'employee_name')

# 1-Crear al menos 3 tablas con dos atributos cada una ----------------------------------------------------------------------------