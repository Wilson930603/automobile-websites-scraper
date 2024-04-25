import scrapy,json
from datetime import datetime
import os
import sys
from tqdm import tqdm
import mysql.connector
from mysql.connector import errorcode
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from random import randint
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By as by
from time import sleep
def get_driver_firefox():
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

    driver = webdriver.Firefox(executable_path = GeckoDriverManager().install(), options=options)
    return driver
class CrawlerSpider(scrapy.Spider):
    download_delay = 0.5
    concurrent_request = 10
    name = 'ca_copart'
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
        # 'CLOSESPIDER_ITEMCOUNT': 50,
        'ITEM_PIPELINES': {
            'automobile.pipelines.AutomobilePipeline': 300,
        },
    }
    def create_connection(self):
        max_retries = 5
        retry_delay = 5

        for _ in range(max_retries):
            try:
                return mysql.connector.connect(
                    host='104.238.228.196',
                    user='root',
                    password='Crawler@2021',
                    database='intermediate_db',
                )
            except mysql.connector.Error as err:
                if err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("Error connecting to MySQL. Retrying...")
                    time.sleep(retry_delay)
                else:
                    print(f"MySQL Error: {err}")
                    break
        else:
            print("Failed to establish MySQL connection after multiple retries.")
            return None
    def execute_query_with_retry(self,query, values):
        attempts = 0
        retry_attempts = 3
        retry_delay = 5
        while attempts < retry_attempts:
            try:
                conn = self.create_connection()
                cursor = conn.cursor()

                cursor.executemany(query, values)
                conn.commit()

                cursor.close()
                conn.close()
                print('Records successfully inserted or updated.')
                self.logger.info('Records successfully inserted or updated.')
                return  # Exit the function if the query is executed successfully

            except mysql.connector.Error as ex:
                print(f'Error occurred: {ex}')
                self.logger.error(f'Error occurred: {ex}')
                attempts += 1
                print(f'Retrying in {retry_delay} seconds...')
                self.logger.error(f'Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)

        print('Exceeded maximum retry attempts. Cannot establish a connection.')
        self.logger.error('Exceeded maximum retry attempts. Cannot establish a connection.')
    def closed(self,reason):
        file_name = sys.argv[-1]
        file = sys.argv[-1]
        if not self.conn.is_connected():
            print("MySQL connection is not available. Reconnecting...")
            self.conn = self.create_connection()
        cursor = self.conn.cursor()
        # Replace 'data.json' with the path to your JSON file
        with open(file, 'r') as file:
            data = json.load(file)

        # Prepare the values for bulk insertion or update
        bulk_values = []
        for item in data:
            vin = item['vin']
            document_type = item['Type']
            sale_date = item['Date']
            source = item['Source']
            lot = item["Lot"]
            details = item['Details']
            bulk_values.append((vin, document_type, sale_date, source, lot, details))

        batch_size = 100
        print('Pushing Records To Database...')
        for i in tqdm(range(0, len(bulk_values), batch_size)):
            batch = bulk_values[i:i+batch_size]
            self.execute_query_with_retry(
                """
                INSERT INTO listing_records (Vin, Type, Date, Source, Lot, Details)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Type = VALUES(Type),
                    Date = VALUES(Date),
                    Source = VALUES(Source),
                    Lot = VALUES(Lot),
                    Details = VALUES(Details)
                """,
                batch
            )
            self.logger.info(f'Inserted batch {i // batch_size + 1}/{len(bulk_values) // batch_size}')
        # Commit the changes to the database
        try:
            self.conn.commit()
            self.conn.close()
        except:
            pass
        os.remove(file_name)
        print("ca_copart Finished...")
    def get_chromedriver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(executable_path = ChromeDriverManager().install(), options=options)
        urls = ['https://www.facebook.com/','https://www.youtube.com/','https://www.gmail.com/']
        driver.get(urls[randint(0,len(urls)-1)])
        return driver
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
        driver = get_driver_firefox()
        self.image_cookies = self.login(driver)
        driver.close()
        headers = {'Host': 'www.copart.ca','Connection': 'keep-alive','Cache-Control': 'max-age=0','Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36','DNT': '1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,tr;q=0.7','Cookie': 'visid_incap_...=put here your visid_incap_ value; incap_ses_...=put here your incap_ses_ value'}
        url = "https://www.copart.ca/public/lots/search-results"
        json_data = {'query': ['*',],'filter': {},'sort': ['auction_date_type desc','auction_date_utc asc',],'page': 0,'size': 100,'start': 0,'watchListOnly': False,'freeFormSearch': False,'hideImages': False,'defaultSort': False,'specificRowProvided': False,'displayName': '','searchName': '','backUrl': '','includeTagByField': {},'rawParams': {}}
        yield scrapy.Request(url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})
            
    def parse_data(self,response):
        json_data=response.meta['json_data']
        print(json_data)
        Data=json.loads(response.text)
        for i,row in enumerate(Data['data']['results']['content']):
            print('\n ------------')
            # Do any thing do you want
            lotno = row.get('ln','')
            myurl = f"https://www.copart.ca/public/data/lotdetails/solr/{lotno}"
            vin = row.get('fv','')
            lotno = row.get('ln','')
            current_bid = row.get('dynamicLotDetails',).get('currentBid','')
            selling_location = row.get('yn','')
            odo_meter = row.get('orr','')
            Highlights = row.get('lcd','')
            primary_damage = row.get('dd','')
            secondary_damage = row.get('sdd','')
            cylinders = row.get('cy','')
            drive_type = row.get('drv','')
            fuel_type = row.get('ft','')
            retail_value = row.get('la','')
            key = row.get('hk','')
            engine_type = row.get('egn','')
            paint = row.get('clr','')
            transmission = row.get('tmtp','')
            body_style = row.get('bstl','')
            title = row.get('ld','')
            images = row.get('tims','')
            seller = row.get('scn','')

            url = f"https://www.copart.ca/lot/{lotno}/{row.get('ldu','')}"

            print(vin)
            current_date = datetime.now().strftime("%Y-%m-%d")
            items = {}
            items["vin"] = vin
            items["source"] = self.name
            items["date"] = current_date
            items["type"] = "salvage"
            items["Lot#"] = lotno
            items["Sale Location"] = selling_location
            items["Listing URL"] = url
            items["Sale Status"] = ''
            items["Current Bid"] = current_bid
            items["Title Code"] = title
            items['Seller'] = seller
            items["Odometer"] = odo_meter
            items["Highlights"] = Highlights
            items["Primary Damage"] = primary_damage
            items["Secondary Damage"] = secondary_damage
            items["Cylinders"] = cylinders
            items['Transmission'] = transmission
            items["Notes"] = ''
            items["Drive"] = drive_type
            items["Documents Type"] = ''
            items["Image URLs"] = images
            items["Fuel"] = fuel_type
            items["Est. Retail Value"] = retail_value
            items["Lane/Item/Grid/Row"] = ''
            items["Keys"] = key
            items["Engine Type"] = engine_type
            items["Color"] = paint  
            items["Body Style"] = body_style
            # https://www.copart.ca/public/data/lotdetails/solr/lotImages/55288943/CAN
            img_url = f"https://www.copart.ca/public/data/lotdetails/solr/lotImages/{lotno}/CAN"
            '''
            To get images and color
            '''
            yield scrapy.Request(
                img_url,
                callback=self.parse_images,
                headers=self.headers_json,
                cookies=self.image_cookies,
                meta={"item":items}
            )

        if len(Data['data']['results']['content'])>0:
            json_data['page']+=1
            json_data['start']=json_data['page']*json_data['size']
            yield scrapy.Request(response.url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})


    def parse_images(self,resposne):
        items = resposne.meta.get('item')
        try:
            data = json.loads(resposne.text)

            try:
                images = " , ".join([img.get('url') for img in data["data"]["imagesList"]["HIGH_RESOLUTION_IMAGE"]])
            except:
                images = " , ".join([img.get('url') for img in data["data"]["imagesList"]["FULL_IMAGE"]])
                

            color = data["data"]["lotDetails"].get('clr')
            items["Color"] = color
            items["Image URLs"] = images
            yield items
            print(f"Success: {items['vin']}")
        except Exception as ex:
            self.logger.error(f"{ex}\nFailed to Get Images and colors for LOT:{items['Lot#']}")

