import pandas as pd

path = '../data_input/sistema_salud.csv'
df = pd.read_csv(path, delimiter=';')

df['apellido'] = 'P ' + df['apellido']

df['DimProfesionales.nombre'] = 'D PS ' + df['DimProfesionales.nombre']
df['DimProfesionales.apellido'] = 'D PS ' + df['DimProfesionales.apellido']

columnas_ids = ['_id', 'id_doctor', 'id_paciente', 'id', 'DimProfesionales.id', 'DimProfesionales.id_especialidad',
                'DimEspecialidades.id', 'FactCitas.id', 'FactCitas.id_centro', 'FactCitas.DimCentros.id',
                'FactCitas.DimCentros.id_localidad', 'FactCitas.DimLocalidades.id','recursos_utilizados']
df[columnas_ids] = df[columnas_ids].fillna(0).astype(int)

path_modificado = '../data_input/sistema_salud_nombres_limpios.csv' 
df.to_csv(path_modificado, index=False, sep=';')

print("Archivo modificado guardado en:", path_modificado)
