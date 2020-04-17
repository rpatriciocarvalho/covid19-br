import scrapy
import datetime
import re

class RioDeJaneiroScraper(scrapy.Spider):
    name = 'RJ'
    start_urls = ['https://coronavirus.rj.gov.br/boletins/']
    state = name
    start_date = datetime.date(2020, 3, 21)

    def parse(self, response):

        # Os primeiros boletins estão em formato diferente dos atuais.
        # Há links para conteúdos que não são boletins.
        # Portanto é necessário filtrar os links para os boletins.
        links = response.xpath('//a[@class="elementor-post__read-more"]/@href').extract()
        
        # Como exemplo coloquei explicitamente o link para um boletim no formato lido pelo scraper.
        links = ['https://coronavirus.rj.gov.br/boletim/boletim-coronavirus-13-04-182-obitos-e-3-221-casos-confirmados-no-rj/']

        datas = []
        for link in links:
            link = link.split('-')
            indice_data = link.index('coronavírus') + 1
            ano, dia, mes = int('2019'), int(dia), int(mes)
            data = datetime.date(ano, mes, dia)
            datas.append(data)

        for link in links:
            yield scrapy.Request(link, self.abre_link)


    def abre_link(self, response):
        
        titulo = response.xpath('.//h1[@class="entry-title"]/text()')[0].extract()
        titulo = titulo.split()

        indice_data = titulo.index('coronavírus')+1
        data = titulo[indice_data]
        data = data.replace('(', '')
        data = data.replace('):', '')
        data = data.split('/')
        dia, mes, ano = int(data[0]), int(data[1]), 2020
        data = datetime.date(ano, mes, dia)

        indice_casos_estado = titulo.index('casos') - 1
        casos_estado = titulo[indice_casos_estado]

        indice_obitos_estado = titulo.index('óbitos') - 1
        obitos_estado = titulo[indice_obitos_estado]
        
        conteudo = response.xpath('.//div[@class="entry-content clear"]').extract()
        conteudo = conteudo[0]
        conteudo = conteudo.replace('</p>', '')
        conteudo = conteudo.split('<p>')

        casos_confirmados_dict = {}
        indice_casos_confirmados = conteudo.index("Os casos confirmados estão distribuídos da seguinte maneira:") + 1    
        casos_confirmados = conteudo[indice_casos_confirmados].split('<br> ')

        for caso in casos_confirmados:
            caso = caso.split(' – ')
            casos_confirmados_dict[caso[0]] = caso[-1]

        obitos_confirmados_dict = {}

        r = re.compile("^Os óbitos.*foram registrados nos seguintes municípios:$")
        lista_filtro = list(filter(r.match, conteudo))
        indice_obitos_confirmados = conteudo.index(lista_filtro[0]) + 1

        obitos_confirmados = conteudo[indice_obitos_confirmados].split('<br> ')

        for obito in obitos_confirmados:
            obito = obito.split(' – ')
            obitos_confirmados_dict[obito[0]] = obito[-1]

        yield {
                'date' : data,
                'state' : self.state,
                'city' : None,
                'place_type' : 'state',
                'confirmed' : casos_estado,
                'deaths' : obitos_estado
            }

        for cidade, numero in casos_confirmados_dict.items():
           
            if( cidade in obitos_confirmados_dict ):
                a = obitos_confirmados_dict[cidade]
            else:
                a =  None

            yield {
                'date' : data,
                'state' : self.state,
                'city' : cidade,
                'place_type' : 'city',
                'confirmed' : numero,
                'deaths' : a
            }