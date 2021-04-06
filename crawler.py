import json
import regex
import requests
import logging
from datetime import datetime

curr_date = datetime.today().strftime("%d-%m-%Y-%Hh%M")

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)-4s %(message)s',
                    filename='./logs/{}.log'.format(curr_date))

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class CRAWLER_WEBMOTORS():

    URL_BASE = "https://www.webmotors.com.br"

    URL_API_CALL = (
        '/api/search/car?url=https://www.webmotors.com.br/'
        'carros%2Festoque%3F&actualPage={}&displayPerPage=48&'
        'order=1&showMenu=true&showCount=true&showBreadCrumb=true&'
        'testAB=false&returnUrl=false'
    )

    user_agent = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/87.0.4280.66 Safari/537.36}'
    )

    RGX_LEILAO = r'leil..?o'

    RGX_ATTR = [
        r'..?nico Dono', r'IPVA Pago', r'Aceita Troca',
        r'Ve..?culo financiado', r'Licenciado', r'Garantia de f..?brica',
        r'Ve..?culo de Colecionador', r'Todas as revis..?es feitas pela concession..?ria',
        r'Todas as revis..?es feitas pela agenda do carro',
        r'Adaptada para pessoas com defici..?ncia', r'Alienado'
    ]

    KEYS_ATTR = [
        'unico_dono', 'ipva_pago', 'aceita_troca',
        'financiado', 'licenciado', 'garantia_fabrica',
        'colecionador', 'revisoes_concessionaria',
        'revisoes_agenda_carro', 'pessoas_deficiencia',
        'alienado'
    ]

    key_remove = [
        'Media', 'PhotoPath', 'ListingType',
        'UniqueId', 'ProductCode', 'Channels', 'HotDeals'
    ]

    nested_key_spec_remove = ['Make', 'Model', 'Version']

    error_count = 0
    warning_count = 0

    def __init__(self):
        self.session = requests.Session()

    @property
    def num_total_cars(self):
        data = self._acess_data(1)
        num_total_cars = data.get("Count")

        return num_total_carsSs

    def _analytics(self,num_cars_retrieved,index):
        logging.info(
            "{} {} veículos salvos.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S"), 
                num_cars_retrieved
            )
        )

        logging.info(
            "{} Foram executadas {} requisições ao servidor. Logs de Warning {}, logs de Erro {}.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S"), index,
                self.warning_count, self.error_count)
        )

    def _save_root(self, data):
        with open('data_root.json', 'w+') as f:
            f.write(json.dumps(data, indent=4, ensure_ascii=False))

    def _save_json(self, data):
        worker_name = self.__class__.__name__.split('_')[1]
        
        if self.flag_test:
            file_name = './data/parcial/{}-{}.json'.format(worker_name,curr_date)
        else:
            file_name = './data/base_completa/{}-{}.json'.format(worker_name,curr_date)

        with open(file_name, 'w') as json_file:
            json_file.write(json.dumps(
                data, indent=4, ensure_ascii=False))

    def _request_page(self, URL_DATA):
        return self.session.get(
            self.URL_BASE + URL_DATA,
            headers={'user-agent': self.user_agent}
        )

    def _convert_response_json(self, response):
        return json.loads(response.text)

    def _acess_data(self, index):
        ''' Access server and returns response as json'''
        
        response = self._request_page(self.URL_API_CALL.format(index))
        
        data = self._convert_response_json(response)

        return data

    def _key_removal(self, car):
        for key in self.key_remove:
            car.pop(key, None)
        # removing selected nested keys
        for nested in self.nested_key_spec_remove:
            car.get('Specification', None).get(nested, None).pop('id', None)

        car.get('Specification', None).get('Color', None).pop('IdPrimary', None)

        car.get('Seller', None).pop('Id', None)
        car.get('Seller', None).pop('AdType', None)
        car.get('Seller', None).pop('BudgetInvestimento', None)
    
    def _build_attribute_keys(self, car):
        ''' Performs key flattening'''

        if car.get('Specification').get('VehicleAttributes'):
            for item in car['Specification']['VehicleAttributes']:
                item_found = False
                for rgx, key in zip(self.RGX_ATTR, self.KEYS_ATTR):
                    if regex.search(rgx, item.get('Name').strip(), flags=regex.I):
                        car[key] = True
                        item_found = True
                        break
                    else:
                        if not car.get(key, None):
                            car[key] = False

            if not item_found:
                logging.warning(
                    "A key '{}' não foi encontrada na lista de atributos. A informação será salvada na key "
                    "'oth_attr'".format(item.get('Name'))
                )
                self.warning_count += 1

                if not car.get('oth_attr', None):
                    car['oth_attr'] = item.get('Name')
                else:
                    car['oth_attr'] = [car['oth_attr'], item.get('Name')]

        car.get('Specification', None).pop('VehicleAttributes', None)
            
    def _treat_hotdeal_key(self,car):

        if car.get('HotDeal', None):
            hot_deal = {}
            count = 1
            for item in car.get('HotDeal', None):
                item.pop('Id', None)
                hot_deal['Value'+str(count)] = item.pop('Value', None)

                count = count + 1
            
    def _look_for_leilao(self,car):
        ''' Verify if the field LongComment contains the term "leilao" '''

        if car.get('LongComment'):
            car['LongComment'] = regex.sub(r'\n','',car.get('LongComment'))
            
            if regex.search(self.RGX_LEILAO, car['LongComment'], regex.I):
                car['contains_leilao'] = True
            else:
                car['contains_leilao'] = False
    
    def _parse_data(self, data):
        ''' Receives the json response. Returns dict containing relevant info of the cars'''
        
        result = []

        self._save_root(data)

        cars = data.get("SearchResults")

        for car in cars:
            self._key_removal(car)

            self._build_attribute_keys(car)

            self._treat_hotdeal_key(car)

            self._look_for_leilao(car)

            result.append(car)
        
        return result

    def get_data_from_website(self, save_root=False, index=1):

        num_total_cars = 0
        num_cars_retrieved = 0
        num_cars_retrieved_prev = 0

        self.flag_test = False

        data_crawled = []

        num_total_cars = self.num_total_cars

        answer = input(
            'Foram encontrados {} carros no servidor.'
            'Deseja extrair toda base de dados de {}?'
            '\nDigite "S" para Sim e "N" para Não\n'.format(num_total_cars,self.URL_BASE)
        )

        if answer == 'N' or answer == 'n':
            self.flag_test = True
            num_total_cars = int(input('Digite o limite de carros: '))
        
        logging.info(
            "{} Iniciando extração de dados do sistema.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S")
            )
        )

        while num_cars_retrieved <= num_total_cars:
            try:
                data = self._acess_data(index)

                data_crawled.extend(self._parse_data(data))

                num_cars_retrieved = len(data_crawled)

                if num_cars_retrieved == num_cars_retrieved_prev:
                    break
                else:
                    num_cars_retrieved_prev = num_cars_retrieved

                logging.info(
                    "{} Número de veículos extraídos: {}.".format(
                        datetime.today().strftime("%d-%m-%Y-%H:%M:%S"),
                        num_cars_retrieved
                    )
                )

                index = index + 1

            except requests.HTTPError:
                logging.error("Erro na comunicação com o servidor")
                self.error_count += 1
                continue

            except Exception:
                logging.error("Erro no Tratamento de Dados")
                self.error_count += 1
                continue

        self._analytics(num_cars_retrieved, index)
        self._save_json(data_crawled)


if __name__ == "__main__":
    crawler = CRAWLER_WEBMOTORS()
    crawler.get_data_from_website(True)
