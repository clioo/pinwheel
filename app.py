 
from bs4 import BeautifulSoup
import json
import sys
import csv
import requests
import datetime


class PinWheelCrawler:

    def __init__(self, debug=False):
        self.web_pages = {
            'irs': ('https://apps.irs.gov/app/picklist/list/'
                    'priorFormPublication.html?resultsPerPage=200&sortColumn=sortOrder&indexOfFirstRow=0&criteria=formNumber&'
                    'value={value}&isDescending=false'),
        }

    # Private methods
    def _export_to_csv(self, data, file_name):
        """Data must be an array of dictionaries so it can export it"""
        if data:
            keys = data[0].keys()
            with open(f'./results/{file_name}_results.csv', 'w') as output_file:
                dict_writer = csv.DictWriter(
                    output_file,
                    fieldnames=keys,
                    lineterminator='\n'
                )
                dict_writer.writeheader()
                dict_writer.writerows(data)

    def _write_json_file(self, data, output):
        with open(output, 'w') as fout:
            json.dump(data, fout)

    def _clean_string(self, cad, replace=''):
        cad = cad.strip()
        cad = cad.replace('\t', replace)
        cad = cad.replace('\n', replace)
        cad = cad.replace(':', replace)
        cad = cad.replace('\xa0', replace)
        return cad

    def _iris_get_data_table(self, soup, value: str):
        """This method only works for beautiful soup 4 and parses a html table
        to a list of dictionaries"""
        rows = soup.select('tr')
        headers = rows[0].select('th')
        rows = rows[1:]
        data = []
        years = []
        for row in rows:
            cells = row.select('td')
            data_item = {}
            for i, cell in enumerate(cells):
                key = self._clean_string(headers[i].get_text())
                data_item[key] = self._clean_string(cell.get_text())
                # only for urls
                has_links = cell.select('a')
                if has_links:
                    data_item[key + '_url'] = cell.select_one('a').get('href')
                if key == 'Revision Date':
                    years.append(int(data_item[key]))
            if data_item['Product Number'] == value:
                url = data_item['Product Number_url']
                name = data_item.get('Product Number')
                year = data_item['Revision Date']
                self._download_file(url, f'./downloads/{name} - {year}.pdf')
                data.append(data_item)
        if data:
            return data, min(years), max(years)
        else:
            return data, 0, 0

    def _download_file(self, url, save_path, chunk_size=128) -> str:
        """Returns the path of the downloaded file"""
        file_name = ''
        with requests.Session() as session:
            request = session.get(url, stream=True)
            with open(save_path, 'wb') as fd:
                for chunk in request.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
                file_name = fd.name
        return file_name

    def _read_config_file(self, key, default=''):
        """Read data from json config file,
        if doesn't find it, returns a default value"""
        data = default
        with open('config.json') as config_file:
            jsonConfig = json.load(config_file)
            data = jsonConfig.get(key, default)
        return data
    
    # Public methods

    def crawl_irs(self):
        # Rules
        available_criteria = {
            'formNumber',
            'title',
            'currentYearRevDateString'
        }
        basic_row = {
            'form_number': '',
            'form_title': '',
            'min_year': '',
            'max_year': ''
        }
        # Locators
        table_locator = 'table.picklist-dataTable'
        

        # Validating params
        search_criteria = self._read_config_file('irs')
        values = search_criteria.get('values')
        assert values and values is not None, "You must enter a list of values in configs"
        criteria = search_criteria.get('criteria')
        assert criteria in available_criteria, (f"{criteria} not available, "
                                                f"available: {available_criteria}")

        results = []
        # crawling
        with requests.session() as session:
            for value in values:
                url = self.web_pages['irs'].format(value=value, criteria=criteria)
                res = session.get(url)
                soup = BeautifulSoup(res.content)
                table = soup.select_one(table_locator)
                data, min_year, max_year = self._iris_get_data_table(table, value)
                if data:
                    row = basic_row.copy()
                    row['form_number'] = value
                    row['form_title'] = data[0].get('Title')
                    row['min_year'] = min_year
                    row['max_year'] = max_year
                    results.append(row)
            self._write_json_file(results, f'irs_results.json')


if __name__ == '__main__':
    pinwheel_crawler = PinWheelCrawler(debug=True)
    crawlers = {
        'irs': pinwheel_crawler.crawl_irs
    }
    try:
        execution = str(sys.argv[1])
    except:
        print('Not args given.')

    # Ensuring execution type exists
    assert execution in crawlers.keys(), ("Crawler not available, "
                                          f"availables: {crawlers.keys()}")

    # Executing crawlers
    crawler = crawlers[execution]
    crawler()

    # pinwheel_crawler.quit_driver()
