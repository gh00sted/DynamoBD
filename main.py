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
    {"client_id": 1, "client_name": "Juan Pérez", "client_status": "Activo"},
    {"client_id": 2, "client_name": "Ana López", "client_status": "Inactivo"},
    {"client_id": 3, "client_name": "Carlos Gómez", "client_status": "Activo"}
]
productos = [
    {"product_id": 101, "product_name": "Laptop HP", "price": 899},
    {"product_id": 102, "product_name": "Mouse Logitech", "price": 29},
    {"product_id": 103, "product_name": "Teclado Mecánico", "price": 79}
]
empleados = [
    {"employee_id": 201, "employee_name": "María Fernández", "department": "Finanzas"},
    {"employee_id": 202, "employee_name": "Pedro Ramírez", "department": "TI"},
    {"employee_id": 203, "employee_name": "Laura Sánchez", "department": "Marketing"}
]

#Insertamos los datos
insert_data.insert_data('TablaClientes', clientes)
insert_data.insert_data('TablaProductos', productos)
insert_data.insert_data('TablaEmpleados', empleados)

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

# 5 - Eliminar un registro de cada tabla   ----------------------------------------------------------------------------

#Importamos la clase
from clases.delete_data import DeleteRecords

#La instanciamos
delete_records = DeleteRecords(dynamodb)

#Realizamos las eliminaciones
#delete_records.set_table('TablaClientes')
#response_cliente = delete_records.delete_record({"client_id": 1, "client_name": "Juan Pérez"})

#delete_records.set_table('TablaProductos')
#response_producto = delete_records.delete_record({"product_id": 101, "product_name": "Laptop HP"})

#delete_records.set_table('TablaEmpleados')
#response_empleado = delete_records.delete_record({"employee_id": 201, "employee_name": "María Fernández"})

#print("\nRespuestas de eliminación:")
#print("TablaClientes:", response_cliente)
#print("TablaProductos:", response_producto)
#print("TablaEmpleados:", response_empleado)

# 6 - Obtener todos los registros de cada tabla   ----------------------------------------------------------------------------

#Importamos la clase
from clases.scan_data import ScanRecords

#La instanciamos
scan_records = ScanRecords(dynamodb)

#Realizamos la consultas
scan_records.set_table('TablaClientes')
todos_clientes = scan_records.scan_table()

scan_records.set_table('TablaProductos')
todos_productos = scan_records.scan_table()

scan_records.set_table('TablaEmpleados')
todos_empleados = scan_records.scan_table()

print("\nTodos los registros de TablaClientes:")
for cliente in todos_clientes:
    print(cliente)

print("\nTodos los registros de TablaProductos:")
for producto in todos_productos:
    print(producto)

print("\nTodos los registros de TablaEmpleados:")
for empleado in todos_empleados:
    print(empleado)
    
# 7 - Obtener una conjunto de registros de un filtrado de cada tabla    ----------------------------------------------------------------------------

#Importamos la clase y Attr de boto3 para las condiciones
from clases.filter_data import FilterRecords
from boto3.dynamodb.conditions import Attr


#La instanciamos
filter_records = FilterRecords(dynamodb)

# Filtrar en TablaClientes: obtener clientes con client_status == "Activo"
filter_records.set_table('TablaClientes')
filtered_clientes = filter_records.filter_records(Attr("client_status").eq("Activo"))

# Filtrar en TablaProductos: obtener productos con price > 100
filter_records.set_table('TablaProductos')
filtered_productos = filter_records.filter_records(Attr("price").gt(100))

# Filtrar en TablaEmpleados: obtener empleados del departamento "TI"
filter_records.set_table('TablaEmpleados')
filtered_empleados = filter_records.filter_records(Attr("department").eq("TI"))

print("\nRegistros filtrados en TablaClientes:")
for item in filtered_clientes:
    print(item)

print("\nRegistros filtrados en TablaProductos:")
for item in filtered_productos:
    print(item)

print("\nRegistros filtrados en TablaEmpleados:")
for item in filtered_empleados:
    print(item)
    
# 8 - Realizar una eliminación condicional de cada tabla    ----------------------------------------------------------------------------
print("")

#Importamos la clase
from clases.conditional_delete_data import ConditionalDeleteRecords

#La instanciamos
conditional_delete = ConditionalDeleteRecords(dynamodb)

# Realizamos una eliminación condicional en cada tabla

# TablaClientes: eliminar registro solo si client_status es "Inactivo"
conditional_delete.set_table('TablaClientes')
try:
    response_client = conditional_delete.conditional_delete_record(
         key={"client_id": 2, "client_name": "Ana López"},
         condition_expression=Attr("client_status").eq("Inactivo")
    )
    print("Registro eliminado en TablaClientes (client_status es Inactivo):", response_client)
except Exception as e:
    print("No se pudo eliminar condicionalmente el registro en TablaClientes:", e)
    
print("")
# TablaProductos: eliminar registro solo si price es menor a 50
conditional_delete.set_table('TablaProductos')
try:
    response_producto = conditional_delete.conditional_delete_record(
         key={"product_id": 102, "product_name": "Mouse Logitech"},
         condition_expression=Attr("price").lt(50)
    )
    print("Registro eliminado en TablaProductos (price es menor a 50):", response_producto)
except Exception as e:
    print("No se pudo eliminar condicionalmente el registro en TablaProductos:", e)

print("")
# TablaEmpleados: eliminar registro solo si department es "Marketing"
conditional_delete.set_table('TablaEmpleados')
try:
    response_empleado = conditional_delete.conditional_delete_record(
         key={"employee_id": 203, "employee_name": "Laura Sánchez"},
         condition_expression=Attr("department").eq("Marketing")
    )
    print("Registro eliminado en TablaEmpleados (department es Marketing):", response_empleado)
except Exception as e:
    print("No se pudo eliminar condicionalmente el registro en TablaEmpleados:", e)
    
# 9 - Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla    ----------------------------------------------------------------------------