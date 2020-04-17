import scrapy
import urllib.request
import zipfile
import shutil
import rows
import datetime
from pathlib import Path

ZIP_URL = 'http://painel.saude.rj.gov.br/arquivos/painel_covid19_erj_ses.zip'
BASE_PATH = Path(__file__).parent
EXTRACAO_PATH = BASE_PATH / "dados_originais_scrapy_rj"

def extrai_data(arquivo):
        
    data = arquivo.split('.')
    data = data[0].split('_')
    dia = data[-3]
    mes = data[-2]
    ano = data[-1]
    pasta_data = ano + mes + dia

    return pasta_data 


def gera_data(pasta):
    
    ano = int(pasta[0:4])
    mes = int(pasta[4:6])
    dia = int(pasta[6:8])
    
    return datetime.date(ano, mes, dia) 


def conta_casos_confirmados(cidades, casos_confirmados):

    total = {}
    for cidade in cidades:
        numero = 0
        
        for caso in casos_confirmados:
            if cidade == caso.munic_residencia.rstrip(): 
                numero += 1
        
        total[cidade] = numero

    return total


def conta_mortes_confirmadas(cidades, mortes_confirmadas):

    total = {}
    for cidade in cidades:
        numero = 0
        
        for caso in mortes_confirmadas:
            if cidade == caso.mun_res.rstrip(): 
                numero += 1
        
        total[cidade] = numero

    return total


def resultado(casos, mortes, data, estado, casos_estado, mortes_estado):

    yield {
                'date' : data,
                'state' : estado,
                'city' : None,
                'place_type' : 'state',
                'confirmed' : casos_estado,
                'deaths' : mortes_estado
    }

    for cidade, n_casos in casos.items():
        
        if( cidade in mortes ):
            n_mortes = mortes[cidade]
        else:
            n_mortes =  None

        yield {
            'date' : data,
            'state' : estado,
            'city' : cidade,
            'place_type' : 'city',
            'confirmed' : n_casos,
            'deaths' : n_mortes
        }


class OrganizaZip():
    
    def baixa_zip(self, url):
        arquivo_zip, _ = urllib.request.urlretrieve(ZIP_URL)
        return arquivo_zip
        

    def extrai_zip(self, arquivo_zip):

        if( zipfile.is_zipfile(arquivo_zip) ):

            with zipfile.ZipFile(arquivo_zip) as arquivo:
                arquivo.extractall(EXTRACAO_PATH)

                with zipfile.ZipFile(EXTRACAO_PATH / arquivo.namelist()[0]) as arquivo_interno:
                    
                    pasta_data = extrai_data(arquivo.namelist()[0])

                    lista_csv = arquivo_interno.namelist()
                    arquivo_interno.extractall(EXTRACAO_PATH / pasta_data)

            shutil.rmtree(EXTRACAO_PATH / 'painel_covid19_erj_ses')
            return lista_csv, pasta_data


class limpa_dados():

    def limpar(self):
        
        cidades = []
        datas = []

        a = OrganizaZip()
        caminho = a.baixa_zip(ZIP_URL)
        lista_todos_csv, pasta = a.extrai_zip(caminho)

        final_data_csv = lista_todos_csv[0].split('_')
        final_data_csv = final_data_csv[-1]

        # Implementar forma de detectar o padrão do nome dos arquivos automaticamente
        # pois a secrataria muda com frequência
        arquivos_padroes = ['BIConfCOVID_ERJ_' + final_data_csv, 'BIOBITOS_Confirmados_ERJ_' + final_data_csv]     

        dados_casos_confirmados = rows.import_from_csv(EXTRACAO_PATH / pasta / arquivos_padroes[0])
        dados_mortes_confirmadas = rows.import_from_csv(EXTRACAO_PATH / pasta / arquivos_padroes[1])

        data_lancamento_dados = gera_data(pasta)

        for caso in dados_casos_confirmados:
            cidades.append(caso.munic_residencia.rstrip())
            datas.append(caso.dt_not)

        tuple(set(cidades))
        tuple(set(datas))

        # Necessário substituir o nome das cidades para o padrão presente
        # em data/populacao-estimada-2019.csv
        resultado_casos = conta_casos_confirmados(cidades, dados_casos_confirmados)
        resultado_mortes = conta_mortes_confirmadas(cidades, dados_mortes_confirmadas)
      
        return {
            'data': data_lancamento_dados,
            'casos': resultado_casos,
            'mortes': resultado_mortes
        }


class RidoDeJaneiroScraper(scrapy.Spider):
    
    name = 'RJ'
    start_urls = []
    state = name

    def start_requests(self):

        dados = limpa_dados()
        resultados = dados.limpar()

        data = str(resultados['data'])
        data = data.split('-')
        dia = data[2]
        mes = data[1]

        url = 'https://coronavirus.rj.gov.br/boletim/boletim-coronavirus-'+dia+'-'+mes

        yield scrapy.Request(url, callback=self.parse, meta=resultados)


    def parse(self, response):

        titulo = response.xpath('.//h1[@class="entry-title"]/text()')[0].extract()
        titulo = titulo.split()
        
        resultados = response.meta

        indice_casos_estado = titulo.index('casos') - 1
        casos_estado = titulo[indice_casos_estado]
        
        indice_obitos_estado = titulo.index('óbitos') - 1
        obitos_estado = titulo[indice_obitos_estado]
        
        return resultado(resultados['casos'], resultados['mortes'], resultados['data'], self.state, casos_estado, obitos_estado)