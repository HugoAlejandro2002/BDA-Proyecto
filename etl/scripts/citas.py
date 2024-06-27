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
    df.to_csv(f'../data_output/citas/{filename}', index=False)

# Tabla de Dimensiones: Pacientes
dim_pacientes_cols = ['id_paciente', 'nombre', 'apellido']
dim_pacientes = extract_unique_rows(df, dim_pacientes_cols)
dim_pacientes.rename(columns={
    'id_paciente': 'id',
    'nombre': 'nombre',
    'apellido': 'apellido'
}, inplace=True)
save_csv(dim_pacientes, 'DimPacientes.csv')

# Tabla de Dimensiones: Profesionales de Salud
dim_profesionales_cols = ['DimProfesionales.id', 'DimProfesionales.nombre', 'DimProfesionales.apellido', 'DimProfesionales.id_especialidad']
dim_profesionales = extract_unique_rows(df, dim_profesionales_cols)
dim_profesionales.rename(columns={
    'DimProfesionales.id': 'id',
    'DimProfesionales.nombre': 'nombre',
    'DimProfesionales.apellido': 'apellido',
    'DimProfesionales.id_especialidad': 'id_especialidad'
}, inplace=True)
save_csv(dim_profesionales, 'DimProfesionales.csv')

# Tabla de Dimensiones: Especialidades
dim_especialidades_cols = ['DimEspecialidades.id', 'DimEspecialidades.nombre']
dim_especialidades = extract_unique_rows(df, dim_especialidades_cols)
dim_especialidades.rename(columns={
    'DimEspecialidades.id': 'id',
    'DimEspecialidades.nombre': 'nombre'
}, inplace=True)
save_csv(dim_especialidades, 'DimEspecialidades.csv')

# Tabla de Dimensiones: Centros de Salud
dim_centros_cols = ['FactCitas.DimCentros.id', 'FactCitas.DimCentros.nombre', 'FactCitas.DimCentros.id_localidad']
dim_centros = extract_unique_rows(df, dim_centros_cols)
dim_centros.rename(columns={
    'FactCitas.DimCentros.id': 'id',
    'FactCitas.DimCentros.nombre': 'nombre',
    'FactCitas.DimCentros.id_localidad': 'id_localidad'
}, inplace=True)
save_csv(dim_centros, 'DimCentros.csv')

# Tabla de Dimensiones: Localidades
dim_localidades_cols = ['FactCitas.DimLocalidades.id', 'FactCitas.DimLocalidades.nombre']
dim_localidades = extract_unique_rows(df, dim_localidades_cols)
dim_localidades.rename(columns={
    'FactCitas.DimLocalidades.id': 'id',
    'FactCitas.DimLocalidades.nombre': 'nombre'
}, inplace=True)
save_csv(dim_localidades, 'DimLocalidades.csv')

# Tabla de Dimensiones: Motivos
dim_motivos_cols = ['motivo']
dim_motivos = extract_unique_rows(df, dim_motivos_cols)
dim_motivos['id'] = range(1, len(dim_motivos) + 1)
dim_motivos.rename(columns={
    'motivo': 'descripcion'
}, inplace=True)
save_csv(dim_motivos, 'DimMotivos.csv')

# Tabla de Dimensiones: Diagnósticos
dim_diagnosticos_cols = ['diagnostico']
dim_diagnosticos = extract_unique_rows(df, dim_diagnosticos_cols)
dim_diagnosticos['id'] = range(1, len(dim_diagnosticos) + 1)
dim_diagnosticos.rename(columns={
    'diagnostico': 'descripcion'
}, inplace=True)
save_csv(dim_diagnosticos, 'DimDiagnosticos.csv')

# Tabla de Dimensiones: Tratamientos
dim_tratamientos_cols = ['tratamiento_recetado.medicamento']
dim_tratamientos = extract_unique_rows(df, dim_tratamientos_cols)
dim_tratamientos['id'] = range(1, len(dim_tratamientos) + 1)
dim_tratamientos.rename(columns={
    'tratamiento_recetado.medicamento': 'tratamiento'
}, inplace=True)
save_csv(dim_tratamientos, 'DimTratamientos.csv')

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
save_csv(dim_tiempo, 'DimTiempo.csv')

# Función para mapear valores de una columna en un DataFrame a sus índices en otro DataFrame
def map_to_id(df, map_df, key_column, value_column):
    mapping = dict(zip(map_df[value_column], map_df['id']))
    return df[key_column].map(mapping)

# Actualizar DataFrame original con los IDs correctos
df['id_motivo'] = map_to_id(df, dim_motivos, 'motivo', 'descripcion')
df['id_diagnostico'] = map_to_id(df, dim_diagnosticos, 'diagnostico', 'descripcion')
df['id_tratamiento'] = map_to_id(df, dim_tratamientos, 'tratamiento_recetado.medicamento', 'tratamiento')
df['id_tiempo'] = map_to_id(df, dim_tiempo, 'fecha_consulta', 'fecha_consulta')

# Tabla de Hechos: FactCitas
fact_citas_cols = [
    '_id', 'id_paciente', 'DimProfesionales.id', 'FactCitas.DimCentros.id', 'id_motivo',
    'id_tiempo', 'id_diagnostico', 'id_tratamiento', 'metricas_salud.peso', 'metricas_salud.talla',
    'metricas_salud.frecuencia_cardiaca', 'metricas_salud.presion_diastolica', 'metricas_salud.presion_sistolica'
]
fact_citas = df[fact_citas_cols].copy()
fact_citas.rename(columns={
    '_id': 'id',
    'id_paciente': 'id_paciente',
    'DimProfesionales.id': 'id_profesional',
    'FactCitas.DimCentros.id': 'id_centro',
    'metricas_salud.peso': 'peso',
    'metricas_salud.talla': 'talla',
    'metricas_salud.frecuencia_cardiaca': 'frecuencia_cardiaca',
    'metricas_salud.presion_diastolica': 'presion_diastolica',
    'metricas_salud.presion_sistolica': 'presion_sistolica'
}, inplace=True)
fact_citas['created_at'] = pd.Timestamp.now()
fact_citas['updated_at'] = pd.Timestamp.now()
fact_citas['disabled_at'] = pd.NaT
save_csv(fact_citas, 'FactCitas.csv')
