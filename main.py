import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

def lista_dicionario (elemento, colunas):
    return dict(zip(colunas, elemento))

def lista (elemento, delimitador="|"):
    return elemento.split(delimitador)

def trata_data (elemento):
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

dengue = (
    pipeline
    |"Leitura do dataset de dengue" >>
    ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(lista)
    | "De lista para dicionário" >> beam.Map(lista_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    |"Descompactar casos de Dengue" >> beam.FlatMap(casos_dengue)
    | "Somar casos de dengue" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
    
)
pipeline.run()
