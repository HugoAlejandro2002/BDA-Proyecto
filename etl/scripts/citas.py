import pandas as pd
from datetime import datetime

# Cargar el archivo CSV original
df = pd.read_csv('../data_input/sistema_salud_recursos.csv', delimiter=';')

# Función para extraer filas únicas y añadir campos de timestamp
def extract_unique_rows(df, columns):
    unique_df = df[columns].drop_duplicates().reset_index(drop=True)
    return unique_df

# Función para guardar DataFrame en archivo CSV
def save_csv(df, filename):
    df.to_csv(f'../data_output/{filename}', index=False)

df['fecha_consulta'] = pd.to_datetime(df['fecha_consulta'], format='%d/%m/%Y %H:%M')
dim_tiempo = df['fecha_consulta'].drop_duplicates().reset_index(drop=True).to_frame()
dim_tiempo['id'] = range(1, len(dim_tiempo) + 1)
dim_tiempo['anio'] = dim_tiempo['fecha_consulta'].dt.year
dim_tiempo['trimestre'] = dim_tiempo['fecha_consulta'].dt.quarter
dim_tiempo['mes'] = dim_tiempo['fecha_consulta'].dt.month
dim_tiempo['dia'] = dim_tiempo['fecha_consulta'].dt.day
dim_tiempo['dia_semana'] = dim_tiempo['fecha_consulta'].dt.day_name()
dim_tiempo['semana_anio'] = dim_tiempo['fecha_consulta'].dt.isocalendar().week
save_csv(dim_tiempo, 'DimTiempo.csv')

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
    'diagnostico': 'diagnostico'
}, inplace=True)
save_csv(dim_diagnosticos, 'DimDiagnosticos.csv')

# Tabla de Dimensiones: Tratamientos
dim_tratamientos_cols = ['tratamiento_recetado.medicamento', 'tratamiento_recetado.frecuencia', 'tratamiento_recetado.duracion']
dim_tratamientos = extract_unique_rows(df, dim_tratamientos_cols)
dim_tratamientos['id'] = range(1, len(dim_tratamientos) + 1)
dim_tratamientos.rename(columns={
    'tratamiento_recetado.medicamento': 'medicamento',
    'tratamiento_recetado.frecuencia': 'frecuencia',
    'tratamiento_recetado.duracion': 'duracion'
}, inplace=True)
save_csv(dim_tratamientos, 'DimTratamientos.csv')

# Tabla de Dimensiones: Recursos Médicos
dim_recursos_medicos_cols = ['recursos_utilizados', 'sistema_salud recursos_medicos.tipo_recurso', 'sistema_salud recursos_medicos.nombre', 'sistema_salud recursos_medicos.estado']
dim_recursos_medicos = extract_unique_rows(df, dim_recursos_medicos_cols)
dim_recursos_medicos.rename(columns={
    'recursos_utilizados': 'id',
    'sistema_salud recursos_medicos.tipo_recurso': 'tipo_recurso',
    'sistema_salud recursos_medicos.nombre': 'nombre',
    'sistema_salud recursos_medicos.estado': 'estado'
}, inplace=True)
dim_recursos_medicos['id'] = dim_recursos_medicos['id'].astype(int)
save_csv(dim_recursos_medicos, 'DimRecursosMedicos.csv')

# Función para mapear valores de una columna en un DataFrame a sus índices en otro DataFrame
def map_to_id(df, map_df, key_column, value_column):
    mapping = dict(zip(map_df[value_column], map_df['id']))
    return df[key_column].map(mapping)

# Actualizar DataFrame original con los IDs correctos
df['id_motivo'] = map_to_id(df, dim_motivos, 'motivo', 'descripcion')
df['id_diagnostico'] = map_to_id(df, dim_diagnosticos, 'diagnostico', 'diagnostico')
df['id_tratamiento'] = map_to_id(df, dim_tratamientos, 'tratamiento_recetado.medicamento', 'medicamento')
df['id_tiempo'] = map_to_id(df, dim_tiempo, 'fecha_consulta', 'fecha_consulta')

# Tabla de Hechos: FactCitas
fact_citas_cols = [
    '_id', 'id_paciente', 'DimProfesionales.id', 'FactCitas.id_centro', 'id_motivo',
    'id_tiempo', 'id_diagnostico', 'id_tratamiento', 'FactCitas.DimLocalidades.id',
    'metricas_salud.talla', 'metricas_salud.peso', 'metricas_salud.frecuencia_cardiaca',
    'metricas_salud.presion_diastolica', 'metricas_salud.presion_sistolica', 'recursos_utilizados'
]
fact_citas = df[fact_citas_cols].copy()
fact_citas.rename(columns={
    '_id': 'id',
    'id_paciente': 'id_paciente',
    'DimProfesionales.id': 'id_profesional',
    'FactCitas.id_centro': 'id_centro',
    'FactCitas.DimLocalidades.id': 'id_localidad',
    'metricas_salud.talla': 'talla',
    'metricas_salud.peso': 'peso',
    'metricas_salud.frecuencia_cardiaca': 'frecuencia_cardiaca',
    'metricas_salud.presion_diastolica': 'presion_diastolica',
    'metricas_salud.presion_sistolica': 'presion_sistolica'
}, inplace=True)
save_csv(fact_citas, 'FactCitas.csv')