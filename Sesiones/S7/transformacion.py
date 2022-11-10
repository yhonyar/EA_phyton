import pandas as pd
from db_credenciales import conexion_repo_preparacion
from prefect import task

@task
def transform():
    
    print('Comienza la transformación')
    
    connection = conexion_repo_preparacion()
    
    sql = "select * from covid19"
    df = pd.read_sql(sql,con=connection)
    
    #eliminar duplicados
    new_df = df.drop_duplicates()
    new_df = new_df.drop_duplicates(df.columns[~df.columns.isin(['index'])],
                        keep='first')
    
    #Eliminar columnas  
    new_df = new_df.drop(columns=["index","Combined_Key","Case_Fatality_Ratio","Lat","Long_"])
    
    #eliminar las filas que tienen valores nulos o ceros en las columnas confirmed or
    #death or recovered or active .
    new_df = new_df.dropna(subset=['Confirmed','Active','Deaths','Recovered'])
    new_df = new_df.drop(new_df[new_df['Confirmed']==0].index)
    
    #agrupar por países
    res_df = df.groupby(["Country_Region"], as_index=False)['Confirmed','Active','Deaths','Recovered'].sum()
    
    print('Termina la transformación')
    
    return new_df, res_df

#transform()
       