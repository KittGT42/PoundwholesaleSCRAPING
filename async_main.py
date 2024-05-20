import csv
import json
import time

import requests
from bs4 import BeautifulSoup
from seleniumwire import webdriver

from selenium.webdriver.common.by import By

import aiohttp
import asyncio

proxy_username = 'yaedxsbe'
proxy_password = 'zbs41w4s8j6g'
seleniumwire_options = {
    'proxy': {
        'http': f'http://{proxy_username}:{proxy_password}@38.154.227.167:5868',
        'verify_ssl': False,
    },
}
start_time = time.time()
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

with webdriver.Chrome(options=options, seleniumwire_options=seleniumwire_options) as driver:
    url = "https://www.poundwholesale.co.uk/brands/"
    driver.get(url)
    driver.implicitly_wait(10)
    brands = [brand.find_element(By.TAG_NAME, 'a').get_attribute('href')
              for brand in driver.find_elements(By.CLASS_NAME, 'brand-name')]


async def get_data_from_page(session, jsn):
    jsn = jsn
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
            async with session.get(product_url, headers=headers) as response:
                product_response = await response.text()
        except aiohttp.ClientConnectionError:
            print("Ошибка подключения к", product_url)
            continue  # Пропускаем этот продукт и переходим к следующему
        except aiohttp.ClientTimeout:
            print("Время ожидания истекло при запросе к", product_url)
            continue  # Пропускаем этот продукт и переходим к следующему
        except aiohttp.ClientResponseError as e:
            print("Ошибка при получении ответа от", product_url, ":", e)
            continue  # Пропускаем этот продукт и переходим к следующему
        except:
            print('Eror but we work')

        product_soup = BeautifulSoup(product_response, "lxml")
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


async def get_data_from_brand():
    async with aiohttp.ClientSession() as session:
        tasks = []
        counter_brands = 0
        for brand in brands:
            counter_brands += 1
            print('')
            flag = True
            counter = 0
            print(f'Work with {brand}')
            while flag is True:
                counter += 1
                print(f'page {counter}')
                try:
                    r = await session.get(
                        f'{brand}?p={counter}', headers=headers
                    )
                except requests.Timeout:
                    print("Время ожидания истекло при запросе к", brand)
                except requests.RequestException as e:
                    print("Ошибка при запросе", e)

                soup = BeautifulSoup(await r.text(), "lxml")

                all_data = soup.find_all("script", {"type": "application/ld+json"})
                len_of_data = len(all_data)
                for data in all_data:
                    jsn = json.loads(data.string)
                    if 'mainEntity' in jsn and len(jsn['mainEntity']['itemListElement']) > 0:
                        task = asyncio.create_task(get_data_from_page(session, jsn=jsn))
                        tasks.append(task)
                    elif 'mainEntity' in jsn and len(jsn['mainEntity']['itemListElement']) == 0:
                        flag = False
                        break
                    elif len_of_data < 5:
                        flag = False
                        break
        await asyncio.gather(*tasks)


def main():
    asyncio.run(get_data_from_brand())
    finish_time = time.time() - start_time
    print(f'Finished in {round(finish_time, 2)} seconds')


if __name__ == "__main__":
    main()
