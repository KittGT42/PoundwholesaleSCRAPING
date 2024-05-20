import csv
import json

import requests
from bs4 import BeautifulSoup
from selenium import webdriver

from selenium.webdriver.common.by import By

headers_csv = ['product_name', 'SKU', 'Product Barcode/ASIN/EAN', 'price', 'quantity']
options = webdriver.ChromeOptions()
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0"
}
options.add_argument(
    'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36')

with open('products1.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(headers_csv)


def get_data_file():
    with webdriver.Chrome(options=options) as driver:
        url = "https://www.poundwholesale.co.uk/brands/"
        driver.get(url)
        driver.implicitly_wait(10)
        brands = [brand.find_element(By.TAG_NAME, 'a').get_attribute('href')
                  for brand in driver.find_elements(By.CLASS_NAME, 'brand-name')]
        for brand in brands:
            flag = True
            counter = 0
            while flag is True:
                counter += 1
                print(f'page {counter}')
                try:
                    r = requests.get(
                    f'{brand}?p={counter}', headers=headers
                    )
                except requests.Timeout:
                    print("Время ожидания истекло при запросе к", brand)
                except requests.RequestException as e:
                    print("Ошибка при запросе к", product_url, ":", e)

                soup = BeautifulSoup(r.text, "lxml")

                all_data = soup.find_all("script", {"type": "application/ld+json"})
                for data in all_data:
                    jsn = json.loads(data.string)
                    if 'mainEntity' in jsn and len(jsn['mainEntity']['itemListElement']) > 0:
                        for product in jsn['mainEntity']['itemListElement']:
                            try:
                                product_name = product['name'].strip()
                                if '&quot;' in product_name:
                                    product_name = product_name.replace('&quot;', "''")
                                elif '&amp;' in product_name:
                                    product_name = product_name.replace('&amp;', "&")
                                elif '&amp' in product_name:
                                    product_name = product_name.replace('&amp', "&")
                            except:
                                product_name = '-'
                            try:
                                product_SKU = product['sku'].strip()
                            except:
                                product_name = '-'
                            try:
                                product_price = product['offers']['price']
                            except:
                                product_name = '-'
                            product_url = product['offers']['url']
                            try:
                                product_response = requests.get(product_url, headers=headers,
                                                                timeout=15)  # Увеличиваем тайм-аут до 10 секунд
                                product_response.raise_for_status()  # Проверяем статус ответа
                            except requests.Timeout:
                                print("Время ожидания истекло при запросе к", product_url)
                                continue  # Пропускаем этот продукт и переходим к следующему
                            except requests.RequestException as e:
                                print("Ошибка при запросе к", product_url, ":", e)
                                continue  # Пропускаем этот продукт и переходим к следующему

                            product_soup = BeautifulSoup(product_response.text, "lxml")
                            try:
                                product_ASIN = product_soup.find('div',
                                                                 class_='value attribute-code-product_barcode').text.strip()
                            except:
                                product_ASIN = '-'
                            try:
                                product_quantity = product_soup.find('span', class_='pack-qty').text.strip().split(' ')[2]
                            except:
                                product_quantity = '-'
                            with open('products1.csv', 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow(
                                    [product_name, product_SKU, product_ASIN, product_price, product_quantity])
                                print('Added product successfully {}'.format(product_name))
                    elif 'mainEntity' in jsn and len(jsn['mainEntity']['itemListElement']) == 0:
                        flag = False
                        break
                    elif len(all_data) < 5:
                        flag = False
                        break



def main():
    get_data_file()


if __name__ == "__main__":
    main()
