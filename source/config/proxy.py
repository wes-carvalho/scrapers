import requests
from lxml import html

class PROXY():
    
    URL_PROXY = 'https://free-proxy-list.net/'
   
    def get_proxy_list(self):
    
        response = requests.get(self.URL_PROXY)

        tree = html.fromstring(response.content)

        # selects proxy table
        table_proxies = tree.xpath('//table[@class = "table table-striped table-bordered"]/tbody')
    
        # selects lines
        proxies = table_proxies[0].findall('tr')

        br_https_proxies = []

        for tr in proxies:
            tds = tr.xpath('td//text()') # convert <tr> to a list of texts
            country = tds[2]

            if country == 'BR':
                # checks if it is a https proxy
                is_https = True if tds[6].strip() == 'yes' else False
                if is_https == True:
                    br_https_proxies.append([tds[0],tds[1]])

        return br_https_proxies
    
    def get_working_proxies(self,proxies, url_test, header):
        
        working_proxies = []

        for proxy_port in proxies:
            n_try = 1

            proxy = {
                'https':f'http://{proxy_port[0]}:{proxy_port[1]}'
            }

            while n_try <= 3:
                try:
                    response = requests.get(
                        url_test,
                        headers=header,
                        proxies = proxy,
                        timeout = 10
                    )

                    working_proxies.append(proxy_port)
                    break
                except:
                    n_try += 1

        return working_proxies
