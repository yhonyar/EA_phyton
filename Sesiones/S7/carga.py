from prefect import task
from db_credenciales import conexion_repo_final
from transformacion import transform

@task
def carga_datos(df):
    print('Comienza la carga en covidt19')    
    con = conexion_repo_final()
    df.to_sql(con=con, name='covid19', if_exists='append', index=False)
    print('Termina la carga en covidt19')

@task
def carga_resumen(df):
    print('Comienza la carga en covidt19_country')
    con = conexion_repo_final()
    df.to_sql(con=con, name='covid19_country', if_exists='append', index=False)
    print('Termina la carga en covidt19_country')
    
    
#df, res_df =  transform()
#carga_datos(df)
#carga_resumen(res_df)
  