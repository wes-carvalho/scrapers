import json
import requests
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(message)s')


class CRAWLER_WEBMOTORS():

    URL_BASE = "https://www.webmotors.com.br"

    URL_API_CALL = (
        '/api/search/car?url=https://www.webmotors.com.br/'
        'carros%2Festoque%3F&actualPage={}&displayPerPage=24&'
        'order=1&showMenu=true&showCount=true&showBreadCrumb=true&'
        'testAB=false&returnUrl=false'
    )

    user_agent = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/87.0.4280.66 Safari/537.36}'
    )

    def __init__(self):
        self.session = requests.Session()

    def _request_page(self, URL_DATA):
        return self.session.get(
            self.URL_BASE + URL_DATA,
            headers={'user-agent': self.user_agent}
        )

    def _treat_data(self, response):
        return json.loads(response.text)

    def get_data_from_website(self):

        index = 1
        num_total_cars = None
        num_cars_retrieved = 0

        data_crawled = []
        key_remove = ['Media', 'PhotoPath', 'ListingType']

        logging.info("Extraindo dados de {}... ".format(self.URL_BASE))

        while num_cars_retrieved < 10000:

            response = self._request_page(
                self.URL_API_CALL.format(index)
            )

            data = self._treat_data(response)

            if not num_total_cars:
                num_total_cars = data.get("Count")

            data_car = data.get("SearchResults")

            for car in data_car:
                for key in key_remove:
                    car.pop(key, None)

                data_crawled.append(car)

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


if __name__ == "__main__":
    crawler = CRAWLER_WEBMOTORS()
    crawler.get_data_from_website()
