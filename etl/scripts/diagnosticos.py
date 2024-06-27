import pandas as pd
from datetime import datetime

# Cargar el archivo CSV original
df = pd.read_csv('../data_input/sistema_salud_nombres_limpios.csv', delimiter=';')

# Función para extraer filas únicas y añadir campos de timestamp
def extract_unique_rows(df, columns):
    unique_df = df[columns].drop_duplicates().reset_index(drop=True)
    unique_df['created_at'] = pd.Timestamp.now()
    unique_df['updated_at'] = pd.Timestamp.now()
    unique_df['disabled_at'] = pd.NaT
    return unique_df

# Función para guardar DataFrame en archivo CSV
def save_csv(df, filename):
    df.to_csv(f'../data_output/diagnosticos/{filename}', index=False)

# Crear DimTiempo
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
save_csv(dim_tiempo, 'DimTiempo3.csv')

# Tabla de Dimensiones: Motivos
dim_motivos_cols = ['motivo']
dim_motivos = extract_unique_rows(df, dim_motivos_cols)
dim_motivos['id'] = range(1, len(dim_motivos) + 1)
dim_motivos.rename(columns={
    'motivo': 'descripcion'
}, inplace=True)
save_csv(dim_motivos, 'DimMotivos2.csv')

# Tabla de Dimensiones: Enfermedades
dim_enfermedades_cols = ['diagnostico']
dim_enfermedades = extract_unique_rows(df, dim_enfermedades_cols)
dim_enfermedades['id'] = range(1, len(dim_enfermedades) + 1)
dim_enfermedades.rename(columns={
    'diagnostico': 'enfermedad'
}, inplace=True)
save_csv(dim_enfermedades, 'DimEnfermedades.csv')

# Función para mapear valores de una columna en un DataFrame a sus índices en otro DataFrame
def map_to_id(df, map_df, key_column, value_column):
    mapping = dict(zip(map_df[value_column], map_df['id']))
    return df[key_column].map(mapping)

# Actualizar DataFrame original con los IDs correctos
df['id_diagnostico'] = map_to_id(df, dim_motivos, 'motivo', 'descripcion')
df['id_enfermedad'] = map_to_id(df, dim_enfermedades, 'diagnostico', 'enfermedad')
df['id_tiempo'] = map_to_id(df, dim_tiempo, 'fecha_consulta', 'fecha_consulta')

# Tabla de Hechos: FactDiagnosticoEnfermedades
fact_diagnostico_enfermedades_cols = [
    '_id', 'id_diagnostico', 'id_enfermedad', 'id_tiempo'
]
fact_diagnostico_enfermedades = df[fact_diagnostico_enfermedades_cols].copy()
fact_diagnostico_enfermedades.rename(columns={
    '_id': 'id'
}, inplace=True)
fact_diagnostico_enfermedades['created_at'] = pd.Timestamp.now()
fact_diagnostico_enfermedades['updated_at'] = pd.Timestamp.now()
fact_diagnostico_enfermedades['disabled_at'] = pd.NaT
save_csv(fact_diagnostico_enfermedades, 'FactDiagnosticoEnfermedades.csv')
