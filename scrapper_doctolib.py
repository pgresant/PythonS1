from curl_cffi import requests
from lxml import html
import json
import csv
import time
import argparse
from bs4 import BeautifulSoup

HEADERS = {
    'authority': 'www.doctolib.fr',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'fr-FR,fr;q=0.9',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
}


FIELDNAMES = [
    'type', 
    'name', 
    'specialty', 
    'url', 
    'address_name', 
    'address_street', 
    'address_postal_code', 
    'address_locality'
]

DATA = []

class DoctolibScraper: 

    def __init__(self): 
        self.s = requests.Session(impersonate="chrome101")
        self.s.headers = HEADERS

    def iter_doctors(self, url, max_page): 
        assert all([url, max_page])
        for i in range(1, max_page+1): 
            u = url + '?page=%s' % i
            print('going page %s' % i)
            time.sleep(1)
            response = self.s.get(u)
            assert response.status_code == 200

            html_content = response.content
            data_soup = BeautifulSoup(html_content, 'html.parser')
            raw_json_data = data_soup.find_all(attrs={"data-id": "removable-json-ld"})
            print(raw_json_data)
            # raw_json_data = doc.xpath("//script[@type='application/ld+json' and contains(text(), 'medicalSpecialty')]")
            if not raw_json_data: 
                print('no more data')
                break

            assert raw_json_data and len(raw_json_data) == 1
            raw_json_data = json.loads(raw_json_data[0].text)
            assert isinstance(raw_json_data, list)

            doctors = raw_json_data
            for doctor in doctors: 
                d = {}
                d['type'] = doctor['@type']
                d['name'] = doctor['name']
                d['specialty'] = doctor['medicalSpecialty']
                d['url'] = 'https://www.doctolib.fr/'+doctor['url']

                _address = doctor['address']
                d['address_name'] = _address['name']
                d['address_street'] = _address['streetAddress']
                d['address_postal_code'] = _address['postalCode']
                d['address_locality'] = _address['addressLocality']

                print('scraped: %s' % d['name'])
                DATA.append(d)

        return DATA

    def write_csv(self, data): 
        filename = 'data_scraping_doctolib_lobstr_io.csv'
        assert data and isinstance(data, list)

        assert filename
        with open(filename, 'w') as f: 
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            for d in DATA: 
                writer.writerow(d)
        print('write csv: complete')



if __name__ == '__main__':
    s = time.perf_counter()
    d = DoctolibScraper()

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--search-url', '-u', type=str, required=False, help='doctolib search url', default='https://www.doctolib.fr/radiologue/lyon')
    argparser.add_argument('--max-page', '-p', type=int, required=False, help='max page to visit', default=2)
    args = argparser.parse_args()

    search_url = args.search_url
    max_page = args.max_page

    assert all([search_url, max_page])
    data = d.iter_doctors(search_url, max_page)
    d.write_csv(data)
    elapsed = time.perf_counter() - s
    elapsed_formatted = "{:.2f}".format(elapsed)
    print("elapsed:", elapsed_formatted, "s")
    print("success")