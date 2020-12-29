import json
import regex
import requests
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(message)s')


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

    def get_data_from_website(self, save_root=False):

        index = 1
        num_total_cars = None
        num_cars_retrieved = 0

        data_root = []
        data_crawled = []

        key_remove = ['Media', 'PhotoPath', 'ListingType',
                      'UniqueId', 'ProductCode', 'Channels', 'HotDeals']
        nested_key_spec_remove = ['Make', 'Model', 'Version']

        logging.info("Extraindo dados de {}... ".format(self.URL_BASE))

        while num_cars_retrieved < 10000:

            response = self._request_page(
                self.URL_API_CALL.format(index)
            )

            data = self._treat_data(response)

            if not num_total_cars:
                num_total_cars = data.get("Count")

            data_car = data.get("SearchResults")

            # removing keys
            for car in data_car:
                for key in key_remove:
                    car.pop(key, None)

                # removing selected nested keys
                for nested in nested_key_spec_remove:
                    car.get('Specification', None).get(
                        nested, None).pop('id', None)

                car.get('Specification', None).get(
                    'Color', None).pop('IdPrimary', None)

                car.get('Seller', None).pop('Id', None)
                car.get('Seller', None).pop('AdType', None)
                car.get('Seller', None).pop('BudgetInvestimento', None)

                # looks for "leilao" term in "LongComment" key
                if car.get('LongComment'):
                    if regex.search(self.RGX_LEILAO, car['LongComment'], regex.I):
                        car['leilao'] = True
                    else:
                        car['leilao'] = False

                data_crawled.append(car)

            if save_root:
                data_root.append(data)

            num_cars_retrieved = num_cars_retrieved + len(data_car)

            logging.info(
                "Número de veículos extraídos: {}".format(
                    num_cars_retrieved)
            )

            index = index + 1

        logging.info(
            "{} veículos salvos no arquivo car_info.json".format(
                num_cars_retrieved)
        )

        with open('./data/car_info.json', 'w') as json_file:
            json_file.write(json.dumps(
                data_crawled, indent=4, ensure_ascii=False))

        if save_root:
            with open('./data/data_root.json', 'w') as json_file:
                json_file.write(json.dumps(
                    data_root, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    crawler = CRAWLER_WEBMOTORS()
    crawler.get_data_from_website(False)
