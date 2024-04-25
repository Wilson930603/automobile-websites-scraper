import scrapy,json
from datetime import datetime
import os
import sys
import ujson
import pandas as pd
import requests
from selenium.webdriver.common.by import By as by
from webdriver_manager.firefox import GeckoDriverManager
from time import sleep
from selenium import webdriver
from random import randint
from tqdm import tqdm
from io import BytesIO
import mysql.connector
import concurrent.futures
import threading
import csv
def get_driver_firefox():
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

    driver = webdriver.Firefox(executable_path = GeckoDriverManager().install(), options=options)
    return driver
class CrawlerSpider(scrapy.Spider):
    download_delay = 0.5
    concurrent_request = 10
    available_lots_lock = threading.Lock()
    name = 'ca_copart_fullVIN'
    csv_filename = f'./Images_sdb_lots/{name}_lots.csv'
    cookies = {'incap_ses_1129_242093': ''}
    headers_json = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0','Accept': 'application/json, text/plain, */*','Accept-Language': 'en-GB,en;q=0.5','X-Requested-With': 'XMLHttpRequest','Access-Control-Allow-Headers': 'Content-Type, X-XSRF-TOKEN','Content-Type': 'application/json','Origin': 'https://www.copart.ca','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Referer': 'https://www.copart.ca/public/data/locations/continents',
        'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    }
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f'./{name}_logs'):
        os.makedirs(f'./{name}_logs')
    custom_settings = {
        'LOG_FILE': f'./{name}_logs/{name}_{current_date}.log',
    }
    
    def return_formated_data(self,item,old_data):
        # Split the string into lines
        
        if old_data.get(item.get('Lot#')):
            lines = old_data[item.get('Lot#')]["Details"].split('\n')

            # Extract keys and values from the lines and create a dictionary
            data_dict = {line.split(': ')[0]: line.split(': ')[1] for line in lines if ': ' in line}
            data_dict["vin"] = item["VIN"]
            data_dict["type"] = old_data[item.get('Lot#')]["Type"]
            data_dict["date"] = old_data[item.get('Lot#')]["Date"]
            data_dict["source"] = old_data[item.get('Lot#')]["Source"]
            data_dict["lot"] = old_data[item.get('Lot#')]["Lot"]
            return data_dict

        return None

    def export_table_to_json_batch(self,connection, table_name, file_path, source, batch_size=1000):
        try:
            print('Exporting data from db for Ca copart')
            self.logger.info('Exporting data from db for Ca copart')
            final_data = []
            with connection.cursor() as cursor:
                # Retrieve data from the table with parameterized query
                query = "SELECT * FROM {} WHERE `Source` = %s;".format(table_name)
                cursor.execute(query, (source,))

                # Get the column names
                column_names = [desc[0] for desc in cursor.description]

                # Write data to JSON file in batches
                json_data = []
                lot_type = {}
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break

                    for row in batch:
                        row_dict = {}
                        for i in range(len(column_names)):
                            row_dict[column_names[i]] = str(row[i])
                        
                        lot_type[row_dict["Lot"]]=row_dict
                        # json_data.append(lot_type)
                    print("Ca Copart_ batch download: {}".format(len(lot_type)))
                    self.logger.info("Ca Copart_ batch download: {}".format(len(lot_type)))
                final_data = lot_type

            print("Data exported to JSON successfully.")
            self.logger.info("Data exported to JSON successfully.")
            return final_data
        except Exception as ex:
            print("Error exporting data: {}".format(ex))
    def append_lot_to_csv(self, lot):
        # Append a single lot to the CSV file
        with open(self.csv_filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([lot])
    def post_images(self,record):
        try:
            images = record["Image URLs"].split(' , ')

            data = {
                'vehicle': json.dumps(record),
            }
            for itr,img in enumerate(images):
                response = requests.get(url=img,headers=self.headers)
                image_data = BytesIO(response.content)
                self.logger.info(f'Lot Number: {record["lot"]} Downoaded Image: {img}')
                data[f"data_files_r{itr}"] = (f"image_{record['lot']}_{itr + 1}.jpg",image_data,'image/jpeg')
            response = requests.post('http://sdb.vindb.org/api/input/multipart-form-data/singular', files=data)
            with self.available_lots_lock:
                if record['lot'] not in self.available_lots:
                    self.append_lot_to_csv(record['lot'])
            # print(f"{response}: Image Posted > Lot Number: {record['lot']}. {response.text}")
            self.logger.info(f"{response}: Image Posted > Lot Number: {record['lot']}. {response.text}")
        except Exception as ex:
            print(f"Post Images Error: {ex}")
            self.logger.error(f"Post Images Error: {ex}")
    def upload_images_parallel(self, records):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.post_images, records)
    def post_records(self,records):
        ret = False
        total_count = len(records)
        valid_count = 0
        invalid_count = 0
        # line separated json string
        lines = []
        for record in records:
            if isinstance(record, str):
                lines.append(record)
            else:
                lines.append(ujson.dumps(record))
        payload = '\n'.join(lines)
        for i in range(3):
            try:
                # r = requests.post('http://cdb1.vindb.org/input/postdata.php', data = {'data' : payload}, timeout=60)
                # r = requests.post('http://sdb-dev.vindb.org/input/postdata.php', data = {'data' : payload}, timeout=60)
                r = requests.post('http://sdb.vindb.org/input/postdata.php', data = {'data' : payload}, timeout=60)
                print(r.text)
                self.logger.info(r.text)
                lines = r.text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith('OK:'):
                        valid_count = int(line[len('OK:'):])
                        invalid_count = total_count - valid_count
                ret = True
                break
            except Exception as e:
                print(e)
                self.logger.error(e)

        return ret, (total_count, valid_count, invalid_count)

    def closed(self,reason):
        saved_file = sys.argv[-1]
        current_directory = os.getcwd()
        print(mysql.connector.__version__)
        DATABASE_NAME='intermediate_db'
        HOST='104.238.228.196'
        username='root'
        password='Crawler@2021'
        cnx = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=username,password=password,charset='utf8')

        tables = ['listing_records']

        interdb_records = self.export_table_to_json_batch(cnx,tables[0],'listing_records_cacopart.json',"ca_copart")
        cnx.close()
        formated_data = []
        if len(interdb_records)>0:
            print('Copart Formating Data')
            self.logger.info('Copart Formating Data')
            df = pd.read_csv("{}".format(saved_file),low_memory=False)
            self.available_lots = pd.read_csv(self.csv_filename,low_memory=False)["lots"].astype(str).tolist()
            for itr in tqdm(range(len(df))):
                lot = str(df.iloc[itr]["Lot#"]).split('.')[0]
                if lot not in self.available_lots:
                    item = {
                        "Lot#":lot,
                        "VIN":df.iloc[itr]["Vin#"]
                    }
                    temp_data = self.return_formated_data(item,interdb_records)
                    if temp_data:
                        formated_data.append(temp_data)


        batch_size = 50
        print(f'Pushing Records To Database. Total Records: {len(formated_data)}... Press Enter to continue')
        self.logger.info('Pushing Records To Database...')
        for i in tqdm(range(0, len(formated_data), batch_size)):
            batch = formated_data[i:i+batch_size]
            # self.post_records(batch)
            try:
                self.upload_images_parallel(batch)
                self.logger.info(f'Inserted batch {i // batch_size + 1}/{len(formated_data) // batch_size}')
            except Exception as ex:
                self.logger.error(f'{ex} batch {i // batch_size + 1}/{len(formated_data) // batch_size}')
        os.remove(saved_file)
        print('Data Pushed.... Cacopart finished')
        self.logger.info('Data Pushed.... Cacopart finished')
        # try:
        #     df = pd.DataFrame({"lots": self.available_lots})
        #     df.to_csv(self.csv_filename, index=False)
        # except Exception as ex:
        #     self.logger.error(f"Error while saving csv: {ex}")
    def login(self,driver):
    
        print('Ca_Copart Loggin In')
        # email = "copart@autoscale.ventures"
        # password = "r)KJpwW-fkS=1"
        email = "copart@vinaudit.com"
        password = "JTkK7#a5zKbM"

        driver.get('https://www.copart.ca/login/')
        sleep(20)
        driver.find_element(by.XPATH,'//input[@data-uname="loginUsernametextbox"]').send_keys(email)
        driver.find_element(by.XPATH,'//input[@data-uname="loginPasswordtextbox"]').send_keys(password)

        driver.find_element(by.XPATH,'//button[@data-uname="loginSigninmemberbutton"]').click()
        sleep(10)
        print('Ca_Copart Loggin In complete')
        try:

            driver.find_element(by.XPATH,'//div[@id="memberTextSubscriptionModal"]//button[@class="close"]').click()
        except:
            pass
        sleep(25)
        driver.find_element(by.XPATH,'//button[@data-uname="homepageHeadersearchsubmit"]').click()
        sleep(25)
        cookies = driver.get_cookies()
        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
        cookies = cookies_dict.copy()
        return cookies
    def start_requests(self):
        headers = {'Host': 'www.copart.ca','Connection': 'keep-alive','Cache-Control': 'max-age=0','Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36','DNT': '1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,tr;q=0.7','Cookie': 'visid_incap_...=put here your visid_incap_ value; incap_ses_...=put here your incap_ses_ value'}

        driver = get_driver_firefox()
        self.cookies = self.login(driver)
        driver.close()
        url = "https://www.copart.ca/public/lots/search-results"
        json_data = {'query': ['*',],'filter': {},'sort': ['auction_date_type desc','auction_date_utc asc',],'page': 0,'size': 100,'start': 0,'watchListOnly': False,'freeFormSearch': False,'hideImages': False,'defaultSort': False,'specificRowProvided': False,'displayName': '','searchName': '','backUrl': '','includeTagByField': {},'rawParams': {}}
        yield scrapy.Request(url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})
        
    def parse_data(self,response):
        json_data=response.meta['json_data']
        print(json_data)
        Data=json.loads(response.text)
        for i,row in enumerate(Data['data']['results']['content']):
            print('\n ------------')
            lotno = row.get('ln','')
            myurl = f"https://www.copart.ca/public/data/lotdetails/solr/{lotno}"
            vin = row.get('fv','')
            lotno = row.get('ln','')

            url = f"https://www.copart.ca/lot/{lotno}/{row.get('ldu','')}"
            if vin.endswith("*"):
                self.logger.info(f"Gotten Star on page: {json_data['page']}")
                return
            print(vin)
            current_date = datetime.now().strftime("%Y-%m-%d")
            items = {}
            items["Vin#"] = vin
            items["Lot#"] = lotno

            yield items
            
        if len(Data['data']['results']['content'])>0:
            json_data['page']+=1
            json_data['start']=json_data['page']*json_data['size']
            yield scrapy.Request(response.url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})

