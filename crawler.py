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

# Fix nata list
# FIx Code org
# List of info messages (resume by the end of exec)
# more changes


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

    def _request_page(self, URL_DATA):
        return self.session.get(
            self.URL_BASE + URL_DATA,
            headers={'user-agent': self.user_agent}
        )

    def _treat_data(self, response):
        return json.loads(response.text)

    def save_root(self, data):
        with open('data_root.json', 'w+') as f:
            f.write(json.dumps(data, indent=4, ensure_ascii=False))

    def _save_json(self, data):
        with open('./data/car_info.json', 'w') as json_file:
            json_file.write(json.dumps(
                data, indent=4, ensure_ascii=False))

    def _build_atr_keys(self, car):
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
        return car

    def get_data_from_website(self, save_root=False, index=1):

        num_total_cars = 0
        num_cars_retrieved = 0
        num_cars_retrieved_prev = 0

        data_crawled = []

        answer = input(
            "Deseja extrair toda base de dados de {}?\nDigite 'S' para Sim e 'N' para Não\n".format(self.URL_BASE)
        )

        if answer == 'S' or answer == 's':
            num_total_cars = 0 # value will be defined latter
        else:
            num_total_cars = int(input('Digite o limite de carros: '))
        
        while num_cars_retrieved <= num_total_cars:

            try:
                response = self._request_page(
                    self.URL_API_CALL.format(index)
                )

                data = self._treat_data(response)

                if not num_total_cars:
                    num_total_cars = data.get("Count")

                    logging.info("Número de veículos encontrado na base de {}: {}".format(
                        self.URL_BASE, num_total_cars))

                    logging.info("{} Iniciando extração de dados... ".format(
                        datetime.today().strftime("%d-%m-%Y-%H:%M:%S")))

                data_car = data.get("SearchResults")

                if save_root:
                    with open('./data/data_root.json', 'w') as json_file:
                        json_file.write(json.dumps(
                            data, indent=4, ensure_ascii=False))

                # removing keys
                for car in data_car:
                    for key in self.key_remove:
                        car.pop(key, None)

                    # removing selected nested keys
                    for nested in self.nested_key_spec_remove:
                        car.get('Specification', None).get(
                            nested, None).pop('id', None)

                    car = self._build_atr_keys(car)
                    car.get('Specification', None).get(
                        'Color', None).pop('IdPrimary', None)

                    car.get('Seller', None).pop('Id', None)
                    car.get('Seller', None).pop('AdType', None)
                    car.get('Seller', None).pop('BudgetInvestimento', None)

                    # treating HotDeal - key
                    if car.get('HotDeal', None):

                        hot_deal = {}
                        count = 1
                        for item in car.get('HotDeal', None):
                            item.pop('Id', None)
                            hot_deal['Value'+str(count)
                                     ] = item.pop('Value', None)

                            count = count + 1

                    # looks for "leilao" term in "LongComment" key
                    if car.get('LongComment'):
                        if regex.search(self.RGX_LEILAO, car['LongComment'], regex.I):
                            car['contains_leilao'] = True
                        else:
                            car['contains_leilao'] = False

                    data_crawled.append(car)

                num_cars_retrieved = num_cars_retrieved + len(data_car)

                if num_cars_retrieved == num_cars_retrieved_prev:
                    break
                else:
                    num_cars_retrieved_prev = num_cars_retrieved

                logging.info(
                    "{} Número de veículos extraídos: {}.".format(datetime.today().strftime("%d-%m-%Y-%H:%M:%S"),
                                                                  num_cars_retrieved)
                )

                index = index + 1

            except requests.HTTPError:
                logging.error("Erro na comunicação com o servidor")

                if len(data_crawled):
                    logging.info(
                        "O dados tratados até o momento da falha serão salvos.\n Número de carros extraídos: {}".format(len(data_crawled)))
                    logging.info(
                        "Índice da página requisitada: {}.".format(index)
                    )
                    self._save_json(data_crawled)

                error_count += 1

            except Exception:
                logging.error("Erro no tratamento de dados.")

                if len(data_crawled):
                    logging.info(
                        "O dados brutos e tratados até o momento da falha serão salvos.\n Número de carros extraídos: {}".format(len(data_crawled)))

                    logging.info(
                        "Índice da página requisitada: {}.".format(index)
                    )
                    self._save_json(data_crawled)
                    self._save_root(data)

                error_count += 1

        logging.info(
            "{} {} veículos salvos no arquivo car_info.json".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S"), num_cars_retrieved)
        )

        logging.info(
            "{} Foram executadas {} requisições ao servidor. Logs de Warning {}, logs de Erro {}.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S"), num_cars_retrieved,
                self.warning_count, self.error_count)
        )
        self._save_json(data_crawled)


if __name__ == "__main__":
    crawler = CRAWLER_WEBMOTORS()
    crawler.get_data_from_website(True)
