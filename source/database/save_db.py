import json
import regex
import hashlib

from .base import CAR, DATA

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime

curr_time = datetime.today().replace(microsecond=0)

class DATABASE():

    index = 0

    # db connection string
    db_url = 'mysql+pymysql://wesley:Localdb@2602#@localhost:3306/scraper'
    engine = create_engine(db_url, echo=False)
    
    # establishes new session
    Session = sessionmaker(bind = engine)
    session = Session() 
    
    # maps the class attributes (table collumns) to the json key 
    # key_relation = {'table_collumn':'json_key'}
    keys_relation = {
        'price' : 'price',
        'motor' : 'motor',
        'valves':'valvulas',
        'traction':'tracao',
        'fuel':'combustivel',
        'alienated':'alienado',
        'good_deal':'good_deal',
        'car_hash' : 'car_hash',
        'licence' : 'licenciado',
        'ipva_paid' : 'ipva_pago',
        'financing' : 'financiado',
        'collection': 'colecionador',
        'first_owner' : 'unico_dono',
        'fipe_percent':'fipe_percent',
        'vip_autopago':'vip_autopago',
        'marketplace' : 'marketplace',
        'cylinder_power':'cilindrada',
        'warranty' : 'garantia_fabrica',
        'long_comment' : 'long_comment',
        'cylinder_pos':'cilindros_disp',
        'accept_exchange' : 'aceita_troca',
        'contains_leilao' : 'contains_leilao',
        'disability_driver': 'pessoas_deficiencia',
        'scheduled_review' : 'revisoes_agenda_carro',
        'dealership_review' : 'revisoes_concessionaria',

        'city' : ['seller','city'],
        'state' : ['seller','state'],
        'hot_deal': ['hot_deal','value'],
        'title': ['specification','title'],
        'armored': ['specification','armored'],
        'seller_type' : ['seller','seller_type'],
        'ports': ['specification','number_ports'],
        'category': ['specification','body_type'],
        'odometer' : ['specification','odometer'],
        'car_delivery' : ['seller','car_delivery'],
        'dealer_score' : ['seller','dealer_score'],
        'exceeded_plan' : ['seller','exceeded_plan'],
        'year_model': ['specification','year_model'],
        'color' : ['specification','color','primary'],
        'version' : ['specification','version','value'],
        'transmission': ['specification','transmission'],
        'troca_com_troco' : ['seller','troca_com_troco'],
        'year_fabrication': ['specification','year_fabrication'],

        'brand' : ['specification','make','value'],
        'model' : ['specification','model','value']
    }

    def query_car(self,hash):
        return self.session.query(CAR).filter_by(car_hash = hash)
    
    def car_signature(self,car_data):
        '''Generates car hash'''
        car_data_str = [str(x) if x else 'null' for x in car_data]
                
        car_str = '-'.join(car_data_str)
        return hashlib.sha256(car_str.encode()).hexdigest()

    def check_key(self,key):
        if 'hot_deal' in key:
            key[1] = 'value'
        
        if not key in self.keys_relation.values():
            print(f'Key {key} não encontrada no banco')
            print(self.keys_relation.values())
            assert(False)
    
    def is_key_added_db(self,car):
        '''' Checks if all keys from the dictionary is present in the database'''
        for key in car:
            if not isinstance(car.get(key), dict):
                self.check_key(key)
            else:
                for sub_key in car.get(key):
                    if not isinstance(car.get(key).get(sub_key), dict):
                        self.check_key([key,sub_key])
                    else:
                        for inner_sub_key in car.get(key).get(sub_key):
                            if not isinstance(car.get(key).get(sub_key).get(inner_sub_key), dict):
                               self.check_key([key,sub_key,inner_sub_key]) 

    def check_class_dict_key_consistency(self,class_attributes):
        '''Check if the class DATA attributes match the keys in the keys_relation mapping'''

        # check 1
        for key in class_attributes:
            if key not in self.keys_relation.keys():
                print(
                    f"Key {key} presente nos atributos da classe, mas não encontrada no mapeamento keys_relation. Cheque a inconsistência"
                )
                assert(False)

        # check 2
        for key in self.keys_relation.keys():
            if not key in class_attributes:
                print(
                    f"Key {key} presente no mapeamento keys_relation, mas não encontrada nos atributos da classe. Cheque a inconsistência"
                    )
                assert(False)

    def save_table_data(self, car_adv, car_hash):
        ''' Saves advertisement data in DATA table in database '''

        data = DATA(
            car_hash = car_hash,
            date = curr_time,
            price = car_adv.get('price'),
            fipe_percent = float(car_adv.get('fipe_percent')) if car_adv.get('fipe_percent') else None,
            odometer = int(car_adv.get('specification').get('odometer')),
            ipva_paid = car_adv.get('ipva_pago'),
            first_owner = car_adv.get('unico_dono'),
            car_delivery = car_adv.get('seller').get('car_delivery'),
            dealer_score = car_adv.get('seller').get('dealer_score'),
            exceeded_plan = car_adv.get('seller').get('exceeded_plan'),
            accept_exchange = 1 if car_adv.get('aceita_troca') else 0,
            troca_com_troco = car_adv.get('seller').get('troca_com_troco'),
            city = car_adv.get('seller').get('city'),
            color = car_adv.get('specification').get('color').get('primary'),
            state = car_adv.get('seller').get('state'),
            seller_type = car_adv.get('seller').get('seller_type'),
            marketplace = self.marketplace,
            long_comment = car_adv.get('long_comment'),
            contains_leilao = car_adv.get('contains_leilao'),
            hot_deal = list(car_adv.get('hot_deal').values())[0] if car_adv.get('hot_deal') else None,
            financing = 1 if car_adv.get('financiado') else 0,
            licence = 1 if car_adv.get('licenciado') else 0,
            warranty = 1 if car_adv.get('garantia_fabrica') else 0,
            dealership_review = 1 if car_adv.get('revisoes_concessionaria') else 0,
            scheduled_review = 1 if car_adv.get('revisoes_agenda_carro') else 0,
            collection = 1 if car_adv.get('colecionador') else 0,
            good_deal = 1 if car_adv.get('good_deal') else 0,
            disability_driver = 1 if car_adv.get('pessoas_deficiencia') else 0,
            alienated = 1 if car_adv.get('alienated') else 0
        )   

        # check json key existence in database
        self.is_key_added_db(car_adv)

        self.session.add(data)
        self.session.commit()

        self.index += 1 

    def save_car(self,data): 
        ''' Saves car data in CAR table in database '''

        # First data related to car only is saved to CAR table.
        # Then the data related to advertisement of that car is save to DATA table.

        # get DATA table attributes     
        class_attributes = list(DATA.__dict__.keys())[5:-5]
        # adds CAR table attributes
        class_attributes.extend(list(CAR.__dict__.keys())[2:-5])

        self.check_class_dict_key_consistency(class_attributes)

        for item in data:
            is_armored = 1 if regex.search(r's',item.get('specification').get('armored'),regex.I) else 0

            car = CAR(
                title = item.get('specification').get('title'),
                brand = item.get('specification').get('make').get('value'),
                model = item.get('specification').get('model').get('value'),
                version = item.get('specification').get('version').get('value'),
                transmission = item.get('specification').get('transmission'),
                category = item.get('specification').get('body_type'),
                fuel =  item.get('combustivel'),
                motor = item.get('motor'),
                cylinder_power = item.get('cilindrada'),
                traction = item.get('tracao'),
                valves = item.get('valvulas'),
                cylinder_pos = item.get('cilindros_disposicao'),
                year_model = int(item.get('specification').get('year_model')),
                year_fabrication = int(item.get('specification').get('year_fabrication')),
                armored = is_armored,
                ports = int(item.get('specification').get('number_ports'))
            )

            class_atributes = list(car.__dict__.values())[1:]

            car.car_hash = self.car_signature(class_atributes)
            
            car_exist = True if self.query_car(car.car_hash).count() == 1 else False

            if not car_exist:
                self.session.add(car)
                self.session.commit()
            
            self.save_table_data(item, car.car_hash)
    
    def save(self, data, marketplace):

        self.marketplace = marketplace

        self.save_car(data)