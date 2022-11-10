import requests
from tqdm.auto import tqdm
import pandas as pd
from db_credenciales import conexion_repo_preparacion

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

import xml.etree.ElementTree as et

def extract_from_xml(file_to_process):
    xtree = et.parse(file_to_process)
    xroot = xtree.getroot()
    dataframe = pd.DataFrame(columns=['nombre','ventas1','ventas'])   
    for person in xroot: 
        nombre = person.find("nombre").text
        ventas1 = float(person.find("ventas1").text)
        ventas = float(person.find("ventas").text)
        dataframe = dataframe.append({"nombre":nombre, "ventas1":ventas1, "ventas":ventas}, ignore_index=True)
    return dataframe


def extract_covid19():

    OWNER = 'CSSEGISandData'
    REPO = 'COVID-19'
    PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
    URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'
    
    response = requests.get(URL)
    con = conexion_repo_preparacion()
    
    print('Conexión a BD exitosa')
    
    i= 0
    for data in tqdm(response.json()):    
        if data['name'].endswith('.csv'):
            file_path = data['download_url']
            dat = extract_from_csv(file_path)
            if i == 0: 
                dat.to_sql(con=con, name='covid19', if_exists='replace')
            else:
                dat.to_sql(con=con, name='covid19', if_exists='append')
            i=i+1
        if i == 10:
            break
        
    print('Extracción y guardado en BD exitosa.',)
            
            
extract_covid19()            
    