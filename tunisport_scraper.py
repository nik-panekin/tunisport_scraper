"""This script performs image scraping from the Tuni Service website located
at https://www.tunisport.es/.
"""
import os
import os.path
import re
import csv
import sys
import time
import logging
import logging.handlers

import requests
from bs4 import BeautifulSoup
import xlsxwriter
from PIL import Image

# Directory name for saving log files
LOG_FOLDER = 'logs'

# Log file name
LOG_NAME = 'scraper.log'

# Full path to the log file
LOG_PATH = os.path.join(LOG_FOLDER, LOG_NAME)

# Maximum log file size
LOG_SIZE = 2 * 1024 * 1024

# Log files count for cyclic rotation
LOG_BACKUPS = 2

# Timeout for web server response (seconds)
TIMEOUT = 5

# Maximum retries count for executing request if an error occurred
MAX_RETRIES = 3

# The delay after executing an HTTP request (seconds)
SLEEP_TIME = 2

# Host URL for the Tuni Service site
HOST_URL = 'https://www.tunisport.es'

# URL for brands list
BRANDS_URL = HOST_URL + '/catalog/chip-tuning'

# HTTP headers for making the scraper more "human-like"
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 6.1; rv:88.0)'
                   ' Gecko/20100101 Firefox/88.0'),
    'Accept': '*/*',
}

# Common text for displaying while script is shutting down
FATAL_ERROR_STR = 'Fatal error. Shutting down.'

# Regular expression for extraction image URL from the CSS style
IMAGE_RE = re.compile(r'^background-image:url\((.*)\)$')

# Characters not allowed in filenames
FORBIDDEN_CHAR_RE = r'[<>:"\/\\\|\?\*]'

# Name of the file where URLs for processed brands are stored
PROCESSED_BRANDS_FILENAME = 'processed_brands.txt'

# Script entry point
def main():
    setup_logging()

    logging.info('Scraping process initialization.')

    try:
        brands = get_brands()
    except Exception as e:
        logging.error('Error while retrieving brands list. ' + str(e) + '\n'
                      + FATAL_ERROR_STR)
        return

    logging.info(f'Total brands count: {len(brands)}')
    processed_brands = load_brand_list()
    logging.info(f'Already scraped brands count: {len(processed_brands)}')

    for brand in brands:
        if brand['url'] not in processed_brands:
            if scrape_brand(brand['url']):
                processed_brands.append(brand['url'])
                save_brand_list(processed_brands)
            else:
                logging.error(FATAL_ERROR_STR)
                return

        if len(processed_brands) > 4:
            break

    logging.info('Scraping process complete.')

# Setting up configuration for logging
def setup_logging():
    logFormatter = logging.Formatter(
        fmt='[%(asctime)s] %(filename)s:%(lineno)d %(levelname)s - %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    if not os.path.exists(LOG_FOLDER):
        try:
            os.mkdir(LOG_FOLDER)
        except OSError:
            logging.warning('Не удалось создать папку для журнала ошибок.')

    if os.path.exists(LOG_FOLDER):
        fileHandler = logging.handlers.RotatingFileHandler(
            LOG_PATH, mode='a', maxBytes=LOG_SIZE, backupCount=LOG_BACKUPS)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)

# Retrieving HTTP GET response implying TIMEOUT and HEADERS
def get_response(url: str, params: dict=None) -> requests.Response:
    """Input and output parameters are the same as for requests.get() function.
    Also retries, timeouts, headers and error handling are ensured.
    """
    for attempt in range(0, MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT,
                             params=params)
        except requests.exceptions.RequestException:
            time.sleep(SLEEP_TIME)
        else:
            time.sleep(SLEEP_TIME)
            if r.status_code != requests.codes.ok:
                logging.error(f'Error {r.status_code} while accessing {url}.')
                return None
            return r

    logging.error(f'Error: can\'t execute HTTP request while accessing {url}.')
    return None

# Retrieve an image from URL and save it to a file
def save_image(url: str, filename: str) -> bool:
    r = get_response(url)

    try:
        with open(filename, 'wb') as f:
            f.write(r.content)
    except OSError:
        logging.error('Error: can\'t save an image to the disk.')
        return False
    except Exception as e:
        logging.error('Error while retrieving an image from URL: ' + str(e))
        return False

    return True

def scrape_page(url: str) -> dict:
    """Scrapes single page with given URL.

    Returns a dict containing page info:
    {
        'previous_caption': str - last but one text element in "breadcrumb"
                                  section;
        'current_caption': str - last text element in "breadcrumb" section;
        'items': list(dict)
    }

    Each item is a dict as follows:
    {
        'caption': str,
        'url': str,
        'image_url': str
    }
    """
    response = get_response(url)
    if response == None:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    previous_caption = soup.find_all('a', class_='breadcrump')[-1].get_text()
    current_caption = soup.find('span', class_='breadcrump__active').get_text()

    result = {
        'previous_caption': previous_caption.strip(),
        'current_caption': current_caption.strip(),
        'items': []
    }

    items = (soup.find('div', class_='col-md-10')
                 .find('div', class_='row')
                 .find_all('a'))
    for item in items:
        new_item = dict()
        new_item['url'] = HOST_URL + item['href']
        new_item['image_url'] = HOST_URL + re.findall(IMAGE_RE,
                                                      item.div['style'])[0]
        new_item['caption'] = item.p.get_text().strip()
        result['items'].append(new_item)

    return result

def get_brands() -> list:
    """Returns the list of all brands. Each list's item is a dict as follows:
    {
        'caption': str,
        'url': str
    }
    """
    response = get_response(BRANDS_URL)
    if response == None:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    brands = []

    items = (soup.find('div', class_='col-md-10')
                 .find('div', class_='row')
                 .find_all('a'))
    for item in items:
        new_item = {
            'url': HOST_URL + item['href'],
            'caption': item.p.get_text().strip(),
        }
        brands.append(new_item)

    return brands

def save_item(item: dict, worksheet: xlsxwriter.worksheet.Worksheet,
              row: int, max_image_width: int) -> int:
    """Saves a given item as an image file and as a row with text and graphical
    data in an Excel worksheet.
    Input:
        item: dict - a dictionary with item data as follows:
            {
                'brand': str,
                'model': str,
                'submodel': str,
                'url': str,
                'image_url': str,
            }
        worksheet: xlsxwriter.worksheet.Worksheet - the Excel worksheet for
            the item to be saved;
        row: int - row number of the Excel worksheet to write the item's data;
        max_image_width: int - previously retrieved maximum item's image width,
            used for column width ajusting.

    Returns:
        new maximum image width (int) or None if an error occured.
    """
    image_name = item['brand'] + '-' + item['model'] + '-' + item['submodel']
    image_name = re.sub(FORBIDDEN_CHAR_RE, '-', image_name)
    image_name = re.sub(r'\s+', '-', image_name)
    image_name = re.sub(r'-+', '-', image_name)
    image_name = re.sub(r'-\.\.\.$', '', image_name)
    image_name += '.' + item['image_url'].split('.')[-1]
    image_path = os.path.join(item['brand'], image_name)

    if not os.path.exists(item['brand']):
        try:
            os.mkdir(item['brand'])
        except OSError:
            logging.error(f"Can't create a folder for brand {item['brand']}.")
            return None

    if not save_image(item['image_url'], image_path):
        return None

    image_width, image_height = Image.open(image_path).size
    max_image_width = max(image_width, max_image_width)

    brand_model = item['brand'] + ' ' + item['model']
    worksheet.write_url(row, 0, item['url'], string=brand_model)
    worksheet.write(row, 1, item['submodel'])
    worksheet.set_row_pixels(row, image_height)
    worksheet.set_column_pixels(2, 2, max_image_width)
    worksheet.insert_image(row, 2, image_path)
    worksheet.write(row, 3, image_name)

    return max_image_width

def scrape_brand(url: str) -> bool:
    """ Scrapes entire single brand for a given URL.
    Returns True on success and False otherwise.
    """
    def finish_brand_scraping():
        try:
            workbook.close()
        except Exception as e:
            logging.error(f'Can\'t save {xlsx_filename} workbook. ' + str(e))
            return False
        logging.info(f'Total items scraped: {row_count}')

    logging.info(f'Beginnig scraping for brand {url}')

    try:
        scraped_page = scrape_page(url)
        xlsx_filename = scraped_page['current_caption'] + '.xlsx'
    except Exception as e:
        logging.error('Scraping initial page for brand failed. ' + str(e))
        return False

    logging.info(f'Total models count: {len(scraped_page["items"])}')

    try:
        workbook = xlsxwriter.Workbook(xlsx_filename)
    except Exception as e:
        logging.error(f'Can\'t create {xlsx_filename} workbook. ' + str(e))
        return False

    bold = workbook.add_format({'bold': True})
    worksheet = workbook.add_worksheet()
    worksheet.write(0, 0, 'Бренд + Авто', bold)
    worksheet.write(0, 1, 'Модель', bold)
    worksheet.write(0, 2, 'Фото', bold)
    worksheet.write(0, 3, 'Название фото', bold)
    row_count = 0
    max_image_width = 0

    try:
        for model in scrape_page(url)['items']:
            logging.info(f'Scraping data for model {model["url"]}')
            model_page = scrape_page(model['url'])
            for submodel in model_page['items']:
                row_count += 1
                item_to_save = {
                    'brand': model_page['previous_caption'],
                    'model': model_page['current_caption'],
                    'submodel': submodel['caption'],
                    'url': submodel['url'],
                    'image_url': submodel['image_url'],
                }
                max_image_width = save_item(item_to_save, worksheet,
                                            row_count, max_image_width)
                # Error while saving item
                if max_image_width == None:
                    finish_brand_scraping()
                    return False
    except Exception as e:
        logging.error('Failure while scraping pages for car model. ' + str(e))
        finish_brand_scraping()
        return False

    finish_brand_scraping()
    return True

# Saves a list of URLs of processed brands
def save_brand_list(brands: list) -> bool:
    try:
        with open(PROCESSED_BRANDS_FILENAME, 'w') as f:
            f.writelines([brand + '\n' for brand in brands])
    except OSError:
        logging.warning('Can\'t save processed brands list.')
        return False

    return True

# Loads previously saved list of URLs of processed brands
def load_brand_list() -> list:
    brands = []

    if os.path.exists(PROCESSED_BRANDS_FILENAME):
        try:
            with open(PROCESSED_BRANDS_FILENAME, 'r') as f:
                brands = f.readlines()
        except OSError:
            logging.warning('Can\'t load processed brands list.')
            return []

    return [brand.strip() for brand in brands]

if __name__ == '__main__':
    main()
