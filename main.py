import json
import requests
import pandas
import datetime
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

@task(max_retries=10, retry_delay=datetime.timedelta(seconds=10))
def extract(url):
    resultado = requests.get(url)
    if not resultado:
        raise Exception('Datos no obtenidos.')
    return json.loads(resultado.content)

@task
def transform(datos):
    usuarios = []
    for usuario in datos:
        usuarios.append({
            'UsuarioID': usuario['userId'],
            'ID': usuario['id'],
            'Titulo': usuario['title'],
            'Completado': usuario['completed']
        })
    return pandas.DataFrame(usuarios)

@task
def load(datos, path):
    datos.to_csv(path, index=False)

schedule = IntervalSchedule(interval=datetime.timedelta(minutes=2))

def prefect_flow(schedule):
    with Flow('etl_flow', schedule=schedule) as flow:
        url = Parameter(name='parametro_url', required=True)
        usuarios = extract(url)
        transformados = transform(usuarios)
        load(transformados, 'datos.csv')
    return flow

flow = prefect_flow(schedule)
#flow.visualize()
flow.run(parameters={
        'parametro_url': 'https://jsonplaceholder.cypress.io/todos'
    })