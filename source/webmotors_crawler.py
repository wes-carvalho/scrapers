from io import StringIO
import os
import json
import time
import regex
import getpass
import logging
import requests

from unicodedata import normalize
from datetime import datetime

from analytics import ANALYTICS
from database.save_db import DATABASE


curr_date = datetime.today().strftime("%d-%m-%Y-%Hh%M")

market_place = 'webmotors'

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)


class CRAWLER_WEBMOTORS():

    URL_BASE = "https://www.webmotors.com.br"

    URL_API_CALL = (
        '/api/search/car?url=https://www.webmotors.com.br/'
        'carros%2Festoque%3F&actualPage={}&displayPerPage=28&'
        'order=1&showMenu=true&showCount=true&showBreadCrumb=true&'
        'testAB=false&returnUrl=false'
    )

    user_agent = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/87.0.4280.66 Safari/537.36}'
    )

    RGX_SINGLE_WORDS = r'[A-Z][^A-Z]*'

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

    n_cars_requested = 3800

    file_path = None
    send_email = None
    save_db = False
    
    saving_path = None
    
    def __init__(self):
        self.session = requests.Session()
    
    def _get_save_path(self):            
        user = getpass.getuser()
        root = f'{os.getcwd().split(user)[0]}{user}'

        documents_path = ['Documentos','Documents']

        for doc in documents_path:
            path = f'{root}/{doc}'

            if os.path.exists(path):
                if not os.path.exists(f'{path}/Scraper/relatorios'):
                    os.mkdir(f'{path}/Scraper')
                    os.mkdir(f'{path}/Scraper/relatorios')
                    os.mkdir(f'{path}/Scraper/relatorios/resumo_base')
                    os.mkdir(f'{path}/Scraper/relatorios/relatorio_carros')

                if not os.path.exists(f'{path}/Scraper/dados'):
                    os.mkdir(f'{path}/Scraper/dados')
                    os.mkdir(f'{path}/Scraper/dados/base_completa')
                    os.mkdir(f'{path}/Scraper/dados/teste')
                
                self.saving_path = f'{path}/Scraper'
        
        if not self.saving_path:
            logging.error(
                f'Não foi possível encontrar o caminho {path}/Scraper.'
            )
            exit()


        logging.info(f"\n***INFORMAÇÕES SERÃO SALVAS EM {path}/Scraper***\n")
    
    @property
    def num_total_cars(self):
        data = self._acess_data(1)
        num_total_cars = data.get("Count")

        return num_total_cars

    def _infos_extracted(self,num_cars_retrieved,index):
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

    def _clean_json(self,data):
        '''Remove duplicates'''
        
        logging.info(
            "\nRemovendo entradas duplicadas."
        )

        # JSON keys to tuples
        for item in data:
            for key in item:
                if type(item.get(key)) == dict:
                    for sub_key in item.get(key): 
                        if type(item.get(key).get(sub_key)) == dict:
                            item[key][sub_key] = tuple(item.get(key).get(sub_key).items())
            
                    item[key] = tuple(item.get(key).items())

        # convert list of tuples to set (removing duplicates)
        data_cleaned = [dict(t) for t in {tuple(d.items()) for d in data}]

        # from list of tuples back to json
        final_data = []

        for item in data_cleaned:
            item_converted = {}

            for key in item:
                if type(item.get(key)) == tuple:
                    key_converted = {}
                    for sub_key in item.get(key):
                        if type(sub_key[1]) == tuple:
                            sub_key_converted = {sub_key[1][0][0]:sub_key[1][0][1]}
                        else:
                            sub_key_converted = sub_key[1]
               
                        key_converted[sub_key[0]] = sub_key_converted

                    item_converted[key] = key_converted
                else:
                    item_converted[key] = item.get(key)

            final_data.append(item_converted)
        
        logging.info(
            "Limpeza de JSON concluída."
        )
        
        return final_data
        
    def _save_root(self, data):
        with open('data_root.json', 'w+') as f:
            f.write(json.dumps(data, indent=4, ensure_ascii=False))

    def _save_json(self, data):

        logging.info(
            "\nSalvando JSON localmente."
        )

        worker_name = self.__class__.__name__.split('_')[1]
        
        if self.flag_test:
            file_name = f'{self.saving_path}/dados/teste/{worker_name}-{curr_date}.json'
        else:
            file_name = f'{self.saving_path}/dados/base_completa/{worker_name}-{curr_date}.json'

        self.file_path = file_name

        with open(file_name, 'w') as json_file:
            json_file.write(json.dumps(
                data, indent=4, ensure_ascii=False))

        logging.info(
            "JSON salvo localmente.\n"
        )

    def _request_page(self, URL_DATA):
        return self.session.get(
            self.URL_BASE + URL_DATA,
            headers={'user-agent': self.user_agent},
            timeout = 10
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
        
        car.get('Prices').pop('SearchPrice')
        car['Price'] = car.get('Prices').get('Price')
        car.pop('Prices')
    
    def _build_attribute_keys(self, car):
        '''Performs key flattening'''

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

        if car.get('HotDeal'):
            hot_deal = {}
            count = 1
            for item in car.get('HotDeal'):
                item.pop('Id', None)
                hot_deal['Value'+str(count)] = item.pop('Value', None)

                count = count + 1
        
            car['HotDeal'] = hot_deal
            
    def _look_for_leilao(self,car):
        ''' Verify if the field long_comment contains the term "leilao" '''

        if car.get('long_comment'):
            car['long_comment'] = regex.sub(r'\n','',car.get('long_comment'))
            
            if regex.search(self.RGX_LEILAO, car['long_comment'], regex.I):
                car['contains_leilao'] = True
            else:
                car['contains_leilao'] = False


    def _remove_acento(self,text):
        return normalize('NFKD', text).encode('ASCII','ignore').decode('ASCII')
    
    def _get_info_from_version(self,car):
        ''' Extract info of combustivel, motor, cilindrada, tração, valvulas and cilindros_disposicao from the version text'''
        
        rgx_combustivel =(
             r'(?P<combustivel>gasolina|..?lcool|diesel|flex|h..?brido|hybrid|gas|el..?trico|tetrafuel|recharge)'
        )
        
        rgx_motor = r'\s(?P<motor>supercharged|\p{L}*turbo\p{L}*|tsi|ingenium|\p{L}*flex\p{L}*)\s'

        versao = car.get('Specification').get('Title')

        # Extract combustível
        m = regex.search(rgx_combustivel, versao, regex.I)
        if m:
            car['combustivel'] = self._remove_acento(m.groupdict().get('combustivel').strip())

            if regex.search(r'recharge', car.get('combustivel'), regex.I):
                car['combustivel'] = 'ELETRICO'    
        else:   
            car['combustivel'] = 'OUTROS'
        
        # Extract motor
        m = regex.search(rgx_motor, versao, regex.I)
        if m:
            car['motor'] = self._remove_acento(m.groupdict().get('motor').strip())  
            
            if regex.search(r'flex.+|.+flex',car.get('motor'), regex.I) and not car.get('combustivel'): 
                car['combustivel'] = 'FLEX'
        else:    
            car['motor'] = 'OUTROS'

        # Extract Cilindrada        
        rgx_cilindrada = r'\d\.\d'

        m = regex.search(rgx_cilindrada, versao)
        if m:
            car['cilindrada'] = m.group()
        
        # Extract Cilindrada        
        rgx_tracao = r'\dx\d|\p{L}\dWD'
        m = regex.search(rgx_tracao, versao, regex.I)
        car['tracao'] = m.group() if m else None

        # Extract Válvulas        
        rgx_valvulas = r'\d{1,2}V'
        m = regex.search(rgx_valvulas, versao, regex.I)
        car['valvulas'] = m.group() if m else None
            
        # cilindros cylinders        
        rgx_cilindros_disp = r'V\d{1,2}'
        m = regex.search(rgx_cilindros_disp, versao, regex.I)
        car['cilindros_disp'] = m.group() if m else None

    def _set_inner_key_pattern(self, inner_dict):
        ''' Receives a inner_car dictionary. Returns a new_dict containing the correct keys' names'''

        new_dict = {}

        for key in inner_dict:
            value = inner_dict.get(key)
            key_name = key 

            match = regex.findall(self.RGX_SINGLE_WORDS,key)

            if isinstance(inner_dict.get(key),dict):                 
                value = {}
                value = self._set_inner_key_pattern(inner_dict.get(key))
                key_name = key.lower()
            else:        
                if len(match)>0:
                    key_name = '_'.join(match).lower()
                        
            new_dict[key_name] = value
        
        return new_dict

    def _set_key_pattern(self,car):
        ''' Receives car dictionary. Returns a new_dict containing the correct keys' names'''

        new_dict = {}
        
        for key in car:
            key_name = key
            value = car.get(key)

            # match words with uppercase letter (forming a list)
            matches = regex.findall(self.RGX_SINGLE_WORDS,key)
            
            if len(matches)>0:
                key_name = '_'.join(matches).lower() # create the correct name
                                
                if isinstance(car.get(key),dict):
                    # in the case the key is a dict, performs the same transf. on the inner keys
                    value = self._set_inner_key_pattern(car.get(key))
                else:
                    value = car.get(key)

            new_dict[key_name] = value

        return new_dict

    def _normalize_dict(self,car):
        '''Strip, remove accent and set values to uppercase'''

        for key in car:
            if isinstance(car.get(key), dict):
                self._normalize_dict(car.get(key))
            elif type(car.get(key)) == str:
                car[key] = self._remove_acento(car.get(key).strip().upper())

    def _parse_data(self, data):
        ''' Receives the json response. Returns dict containing relevant info of the cars'''
        
        result = []

        cars = data.get("SearchResults")
        
        for car in cars:
            self._key_removal(car)

            self._build_attribute_keys(car)

            self._treat_hotdeal_key(car)

            self._get_info_from_version(car)

            car_info = self._set_key_pattern(car)
            
            self._normalize_dict(car_info)
            
            self._look_for_leilao(car_info)

            result.append(car_info)
        
        return result

    def get_data_from_website(self, save_root=False, index=1):

        num_total_cars = 0
        num_cars_retrieved = 0
        num_cars_retrieved_prev = 0

        self.flag_test = False

        data_crawled = []

        num_total_cars = self.num_total_cars

        answer = input(
            '\nForam encontrados {} carros no servidor.'
            'Deseja extrair toda base de dados de {}?'
            '\nDigite "S" para Sim e "N" para Não\n'.format(num_total_cars,self.URL_BASE)
        )

        if answer == 'N' or answer == 'n':
            self.flag_test = True
            self.send_email = False
            num_total_cars = int(input('\nDigite o limite de carros: '))
        else:
            email_msg = input(
            '\nDeseja enviar e-mail o report gerado por e-mail?'
            '\nDigite "S" para Sim e "N" para Não\n'
            )
            self.send_email = True if email_msg in ['S','s'] else False

        answer = input(
            '\nDeseja salvar as informações no Banco de Dados?'
            '\nDigite "S" para Sim e "N" para Não\n'
        )

        self.save_db = True if answer.lower() == 's' else False

        self._get_save_path()

        logging.info(
            "{} Iniciando extração de dados do sistema.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S")
            )
        )

        while num_cars_retrieved <= num_total_cars:
            try:

                if num_cars_retrieved > self.n_cars_requested:
                    print('Waiting...')
                    time.sleep(300)
                    self.n_cars_requested += 3800
                    print('Ready!')

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

            except (AttributeError, TypeError, requests.exceptions.ChunkedEncodingError,requests.exceptions.ReadTimeout) as e:
                logging.error(f"Resposta inesperada. Replicando requisição {index}. Erro: {e}")
                continue
            except (json.decoder.JSONDecodeError, requests.exceptions.ConnectionError) as e:
                logging.error(f"Bloqueado pelo servidor. Replicando requisição {index} após sleep.time.")
                time.sleep(300)
                index = index - 1
            except Exception:
                logging.error("Erro não identificado na extração de dados")
                raise Exception

        self._infos_extracted(num_cars_retrieved, index)

        data = self._clean_json(data_crawled)
        self._save_json(data)

        return data, self.file_path


if __name__ == "__main__":
    crawler = CRAWLER_WEBMOTORS()
    json_list, file_path = crawler.get_data_from_website(False)
    
    wk = ANALYTICS()
    resumo_path = wk.descriptive_statistics(json_list, file_path)

    files = resumo_path

    if crawler.send_email == True:
       wk.send_email(files)

    if crawler.save_db == True:

        logging.info(f"\nSalvando Informações no Banco\n")

        database = DATABASE()
        n_cars = database.save(json_list, market_place)

        logging.info(f"\n{n_cars} salvos no banco de dados!\n")


