import pandas as pd
from sqlalchemy import create_engine

# Configuración de la base de datos
db_user = 'admin'
db_password = 'Notepad2023'
db_host = 'sistema-salud.c4skyovuhs5j.us-east-1.rds.amazonaws.com'
db_name = 'citas_db'

# Crear la conexión a la base de datos
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}')

# Cargar los archivos CSV
csv_files = {
    'DimCentros': 'data_output/DimCentros.csv',
    'DimEspecialidades': 'data_output/DimEspecialidades.csv',
    'DimLocalidades': 'data_output/DimLocalidades.csv',
    'DimPacientes': 'data_output/DimPacientes.csv',
    'DimProfesionales': 'data_output/DimProfesionales.csv',
    'DimMotivos': 'data_output/DimMotivos.csv',
    'DimDiagnosticos': 'data_output/DimDiagnosticos.csv',
    'DimTratamientos': 'data_output/DimTratamientos.csv',
    'DimRecursosMedicos': 'data_output/DimRecursosMedicos.csv',
    'DimTiempo': 'data_output/DimTiempo.csv',
    'FactCitas': 'data_output/FactCitas.csv'
}

# Función para cargar un CSV a una tabla de MySQL
def load_csv_to_mysql(table_name, csv_path):
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f'Tabla {table_name} cargada con éxito desde {csv_path}')

# Cargar cada archivo CSV en su tabla correspondiente
for table, csv_path in csv_files.items():
    load_csv_to_mysql(table, csv_path)
