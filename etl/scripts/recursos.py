import pandas as pd
from datetime import datetime

# Cargar el archivo CSV original
df = pd.read_csv('../data_input/sistema_salud_recursos.csv', delimiter=';')

# Función para extraer filas únicas y añadir campos de timestamp
def extract_unique_rows(df, columns):
    unique_df = df[columns].drop_duplicates().reset_index(drop=True)
    unique_df['created_at'] = pd.Timestamp.now()
    unique_df['updated_at'] = pd.Timestamp.now()
    unique_df['disabled_at'] = pd.NaT
    return unique_df

# Función para guardar DataFrame en archivo CSV
def save_csv(df, filename):
    df.to_csv(f'../data_output/recursos/{filename}', index=False)

# Crear DimTiempo2
df['fecha_consulta'] = pd.to_datetime(df['fecha_consulta'], format='%d/%m/%Y %H:%M')
dim_tiempo = df['fecha_consulta'].drop_duplicates().reset_index(drop=True).to_frame()
dim_tiempo['id'] = range(1, len(dim_tiempo) + 1)
dim_tiempo['anio'] = dim_tiempo['fecha_consulta'].dt.year
dim_tiempo['trimestre'] = dim_tiempo['fecha_consulta'].dt.quarter
dim_tiempo['cuatrimestre'] = ((dim_tiempo['trimestre'] - 1) // 2) + 1
dim_tiempo['mes'] = dim_tiempo['fecha_consulta'].dt.month
dim_tiempo['dia'] = dim_tiempo['fecha_consulta'].dt.day
dim_tiempo['created_at'] = pd.Timestamp.now()
dim_tiempo['updated_at'] = pd.Timestamp.now()
dim_tiempo['disabled_at'] = pd.NaT
save_csv(dim_tiempo, 'DimTiempo2.csv')

# Tabla de Dimensiones: Tratamientos
dim_tratamientos_cols = ['tratamiento_recetado.frecuencia', 'tratamiento_recetado.duracion']
dim_tratamientos = extract_unique_rows(df, dim_tratamientos_cols)
dim_tratamientos['id'] = range(1, len(dim_tratamientos) + 1)
dim_tratamientos.rename(columns={
    'tratamiento_recetado.frecuencia': 'frecuencia',
    'tratamiento_recetado.duracion': 'duracion'
}, inplace=True)
save_csv(dim_tratamientos, 'DimTratamientos2.csv')

# Tabla de Dimensiones: Medicamentos
dim_medicamentos_cols = ['tratamiento_recetado.medicamento']
dim_medicamentos = extract_unique_rows(df, dim_medicamentos_cols)
dim_medicamentos['id'] = range(1, len(dim_medicamentos) + 1)
dim_medicamentos.rename(columns={
    'tratamiento_recetado.medicamento': 'medicamento'
}, inplace=True)
dim_medicamentos['descripcion'] = pd.NaT  # Agregar descripción si es necesario
save_csv(dim_medicamentos, 'DimMedicamentos.csv')

# Tabla de Dimensiones: Insumos Médicos
dim_insumos_medicos_cols = ['sistema_salud recursos_medicos._id', 'sistema_salud recursos_medicos.tipo_recurso', 'sistema_salud recursos_medicos.nombre', 'sistema_salud recursos_medicos.estado']
dim_insumos_medicos = extract_unique_rows(df, dim_insumos_medicos_cols)
dim_insumos_medicos.rename(columns={
    'sistema_salud recursos_medicos._id': 'id',
    'sistema_salud recursos_medicos.tipo_recurso': 'tipo_recurso',
    'sistema_salud recursos_medicos.nombre': 'nombre',
    'sistema_salud recursos_medicos.estado': 'estado'
}, inplace=True)
dim_insumos_medicos['id'] = dim_insumos_medicos['id'].fillna(0).astype(int)
dim_insumos_medicos['fabricante'] = pd.NaT  # Agregar fabricante si es necesario
dim_insumos_medicos['modelo'] = pd.NaT  # Agregar modelo si es necesario
save_csv(dim_insumos_medicos, 'DimInsumosMedicos.csv')

# Función para mapear valores de una columna en un DataFrame a sus índices en otro DataFrame
def map_to_id(df, map_df, key_column, value_column):
    mapping = dict(zip(map_df[value_column], map_df['id']))
    return df[key_column].map(mapping)

# Actualizar DataFrame original con los IDs correctos
df['id_tratamiento'] = map_to_id(df, dim_tratamientos, 'tratamiento_recetado.frecuencia', 'frecuencia')
df['id_medicamento'] = map_to_id(df, dim_medicamentos, 'tratamiento_recetado.medicamento', 'medicamento')
df['id_insumo_medico'] = map_to_id(df, dim_insumos_medicos, 'sistema_salud recursos_medicos.nombre', 'nombre')
df['id_tiempo'] = map_to_id(df, dim_tiempo, 'fecha_consulta', 'fecha_consulta')

# Tabla de Hechos: FactRecursos
fact_recursos_cols = [
    '_id', 'id_tratamiento', 'id_medicamento', 'id_tiempo'
]
fact_recursos = df[fact_recursos_cols].copy()
fact_recursos.rename(columns={
    '_id': 'id'
}, inplace=True)
fact_recursos['created_at'] = pd.Timestamp.now()
fact_recursos['updated_at'] = pd.Timestamp.now()
fact_recursos['disabled_at'] = pd.NaT
save_csv(fact_recursos, 'FactRecursos.csv')