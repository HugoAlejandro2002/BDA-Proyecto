import pandas as pd

path = '../data_input/sistema_salud.csv'
df = pd.read_csv(path, delimiter=';')

df['apellido'] = 'P ' + df['apellido']

df['DimProfesionales.nombre'] = 'D PS ' + df['DimProfesionales.nombre']
df['DimProfesionales.apellido'] = 'D PS ' + df['DimProfesionales.apellido']

path_modificado = '../data_input/sistema_salud_nombres_limpios.csv' 
df.to_csv(path_modificado, index=False, sep=';')

print("Archivo modificado guardado en:", path_modificado)
