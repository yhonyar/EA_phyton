from prefect import Flow
from transformacion import transform
from carga import carga_datos,carga_resumen
from prefect.schedules import IntervalSchedule
from datetime import timedelta

scheduler = IntervalSchedule(
    interval=timedelta(seconds=10)
)

#with Flow("proceso-ETL") as flow:
with Flow("proceso-ETL", schedule=scheduler) as flow:
    
    # Define tasks
    df, res_df =  transform()
    carga_datos(df)
    carga_resumen(res_df)
    
flow.run()

