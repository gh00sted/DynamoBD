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
#table1 = data_tables.create_table('TablaClientes', 'client_id', 'client_name')
#table2 = data_tables.create_table('TablaProductos', 'product_id', 'product_name')
#table3 = data_tables.create_table('TablaEmpleados', 'employee_id', 'employee_name')

# 2-Crear tres registros encada tabla ----------------------------------------------------------------------------

#Importamos la clase para crear registros
from clases.write_data import InsertData

#Instanciamos
insert_data = InsertData(dynamodb)

#Generamos los datos que queremos introducir
# Datos TablaClientes
clientes = [
    {"client_id": 1, "client_name": "Juan Pérez"},
    {"client_id": 2, "client_name": "Ana López"},
    {"client_id": 3, "client_name": "Carlos Gómez"}
]

# Datos TablaProductos
productos = [
    {"product_id": 101, "product_name": "Laptop HP"},
    {"product_id": 102, "product_name": "Mouse Logitech"},
    {"product_id": 103, "product_name": "Teclado Mecánico"}
]

# Datos TablaEmpleados
empleados = [
    {"employee_id": 201, "employee_name": "María Fernández"},
    {"employee_id": 202, "employee_name": "Pedro Ramírez"},
    {"employee_id": 203, "employee_name": "Laura Sánchez"}
]

#Insertamos los datos
#insert_data.insert_data('TablaClientes', clientes)
#insert_data.insert_data('TablaProductos', productos)
#insert_data.insert_data('TablaEmpleados', empleados)

# 3-Obtener un registro de cada tabla  ----------------------------------------------------------------------------

#Importamos la clase para obtener registros
from clases.get_data import DataRecords 

#Instanciamos
data_records = DataRecords(dynamodb)

#Obtenemos los registros llamando a la funcion
data_records.set_table('TablaClientes', 'client_id', 'client_name')
cliente = data_records.get_record(1, "Juan Pérez") 

data_records.set_table('TablaProductos', 'product_id', 'product_name')
producto = data_records.get_record(101, "Laptop HP")

data_records.set_table('TablaEmpleados', 'employee_id', 'employee_name')
empleado = data_records.get_record(201, "María Fernández")

print("\nRegistros obtenidos:")
print("Cliente:", cliente)
print("Producto:", producto)
print("Empleado:", empleado)

# 5 - Eliminar un registro de cada tabla (1 punto)   ----------------------------------------------------------------------------



