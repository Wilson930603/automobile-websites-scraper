import math
import sys
import scrapy,json,re,os,platform,requests
from tqdm import tqdm
import mysql.connector 
from datetime import datetime
# pip install 2captcha-python
from twocaptcha import TwoCaptcha
from parsel import Selector
from selenium import webdriver
from time import sleep
from selenium.webdriver.common.by import By as by
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support import expected_conditions as EC
import ujson
import pandas as pd
from io import BytesIO
import mysql.connector
import concurrent.futures
import requests
import copy
import time
import shutil
from random import randint
import threading
email = 'iaai@vinaudit.com'
password = '9qSaFcC'
 

class CrawlerSpider(scrapy.Spider):
    name = 'iaai_2'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f'./{name}_logs'):
        os.makedirs(f'./{name}_logs')
    custom_settings = {
        # 'CLOSESPIDER_ITEMCOUNT': 500,
        'LOG_FILE': f'./{name}_logs/{name}_{current_date}.log',
    }
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'

    # main_url='https://www.iaai.com'
    main_url = "https://www.iaai.com/Search"
    baseUrl = "https://www.iaai.com"

    api_key='d368d93ac4830dc4a52b5351a7b92626'
    cookies = {'visid_incap_2807936': 'QfsahuVCQ7a9M4lKKqXtW+X81GQAAAAAQkIPAAAAAACAhjeuAWjPmxb+i2ZtImAKAB/hYd9AQRX+','incap_ses_225_2807936': 'YnfkCdhDjixTaZ+D4VwfA+X81GQAAAAAdSDMyf11CalgBXRzNbJqXg==','_ga_8J4GTR5B9Q': 'GS1.1.1691683545.2.0.1691683545.60.0.0','nlbi_2807936': 'vUNxZom9eDah0DUxxRLPjgAAAAAg6wUhvig9HdQXS7hHpDEw','nlbi_2807936_2147483392': 'K1NhZwpceWsqZFTkxRLPjgAAAACAH3BpwqqpZj+vGcuZmXlV','reese84': '3:lPStjXfxYTLMQYyZjYhTUw==:TpcyP/pJCT5FTw8bpxbJftP/2eQlEoQJsgBLppfMiMyxlXGIcbgeZwuX/KwuPkVDsYUnEZ8HqVk0bHPP1uDvdyx5a5DP91K5qxhYEpOZJNFYJg1rgj3sS+GQO06vhsrGpJyT4fVpIpsV6q6w0lE8b5LUmRYJc+QC25fskayM8rEFS3POk8CjWMzPbKst0WhjbOXQooRmhtnAbnyVy6MMpnXcb2A0ujki5RlPMms+5tCYMOsiFH6C2O/HT/DKyb8+UUCbZm/hP9W9NQ1OR75GPaCk8Nv6P2l22YMMGlegd7uiO/1Qc0i/BIoNCQLXikjyIw2qRJYY88kNoWhzj074WslcboNbTA08v2VTdl5VEWLcvtnibHXy6P4aIFenPjaDVlAWJKomXqBWEOqWIGScQQw/yInHbTz8ZJR7W3WLm5soFgzP2bg4kTnYUELaPzWD+JM666Atqa6P9hzsfQdO3iFV6oeFfR4EQywHK1uBVCjKBRKLXFIAfgpXouf8wF84:c3Pp0vB9HnBmKyXAwg9C6a7/soDdDuAs/c+U6iPBELo=','incap_sh_2807936': 'IAvVZAAAAACg/65ZDAAIoJbUpgYQ2JXUpgb4OMxi+fZ1+UkEL4q3H2Z2','X-Forwarded-For_IpAddress': '104.207.155.22^%^2C^%^20198.143.33.4^%^3A35098','BrokerPopupIPAddress': '1758436118-104.207.155.22-1758436118','BrokerPopupCountryCode': 'NotFound','IAAITrackingCookie': '94be7dd8-532a-412c-9892-6466c4a52365','_sfid_4446': '{^%^22anonymousId^%^22:^%^22eeec07cfa26ac83b^%^22^%^2C^%^22consents^%^22:^[^]}','_evga_4ff5': '{^%^22uuid^%^22:^%^22eeec07cfa26ac83b^%^22}','TimeZoneMapID': '120','mdLogger': 'false','kampyle_userid': 'a4a4-be0f-6d8d-503f-0c0e-a17f-cee5-4694','kampyleUserSession': '1691681659597','kampyleSessionPageCounter': '1','kampyleUserSessionsCount': '1','OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Aug+10+2023+15^%^3A34^%^3A22+GMT^%^2B0000+(Coordinated+Universal+Time)&version=202306.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=https^%^3A^%^2F^%^2Fwww.iaai.com^%^2F&groups=C0001^%^3A1^%^2CC0003^%^3A1^%^2CC0002^%^3A1^%^2CC0004^%^3A1','actualOptanonConsent': '^%^2CC0001^%^2CC0003^%^2CC0002^%^2CC0004^%^2C','_gcl_au': '1.1.820744503.1691681661','_ga': 'GA1.2.772816893.1691681662','_uetsid': '619eb0e0379311ee993cd9dc7cd9dfc8','_uetvid': '619ed5f0379311eebe1e6fa219c15d3b','_gid': 'GA1.2.2003766068.1691681663','ln_or': 'eyIyMzg4ODk3IjoiZCJ9','_fbp': 'fb.1.1691681664609.1972545375','_clck': '1wec7y^|2^|fe1^|0^|1317','_clsk': '12wiohg^|1691681665405^|1^|0^|i.clarity.ms/collect'}
    def recaptcha_v2(self,sitekey, url,api_key):
        try:
            solver = TwoCaptcha(api_key)
            result = solver.solve_captcha(site_key=sitekey, page_url=url)
            # result = solver.recaptcha(sitekey=sitekey, url=url)
        except Exception as e:
            # print(e)
            result = ""
        return result
    def pass_captcha(self,url,post_url,sitekey,api_key):
        CHK=False
        RUN=0
        while CHK==False and RUN<10:
            kq=self.recaptcha_v2(sitekey, url,api_key)
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Accept-Language': 'en-US,en;q=0.5','Content-Type': 'application/x-www-form-urlencoded','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1',}
            data = {'recaptcha-token':sitekey,'g-recaptcha-response': str(kq["code"])}
            response = requests.post(post_url, headers=headers, json=data)
            CK=(response.cookies.get_dict())
            self.cookies.update(CK)
            response = requests.get(self.main_url,cookies=self.cookies)
            if '_Incapsula_Resource?SWUDNSAI=' in response.text:
                print("UnSuccess !!!")
                RUN+=1
            else:
                CHK=True
                print("Success !!!")

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
            print('Exporting data from db for iaai')
            self.logger.info('Exporting data from db for iaai')
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
                    print("IAAI_ batch download: {}".format(len(lot_type)))
                    self.logger.info("IAAI_ batch download: {}".format(len(lot_type)))
                final_data = lot_type
                

            print("Data exported to JSON successfully.")
            self.logger.info("Data exported to JSON successfully.")
            return final_data
        except Exception as ex:
            print("Error exporting data: {}".format(ex))
            self.logger.error("Error exporting data: {}".format(ex))
    
    def post_images(self,record):
        #for record in records:
        try:
            images = record["Image URLs"].split(" , ")

            data = {
                'vehicle': json.dumps(record),
            }
            for itr,img in enumerate(images):
                print(f'Lot Number: {record["lot"]} Downoading Image: {img}')
                self.logger.info(f'Downoading Image: {img}')
                response = requests.get(img.replace('&width=161&height=120','&width=500&height=500'))
                image_data = BytesIO(response.content)
                data[f"data_files_r{itr}"] = (f"image_{itr + 1}.jpg",image_data,'image/jpeg')
            response = requests.post('http://sdb-dev.vindb.org/api/input/multipart-form-data/singular', files=data)
            print(response)
            print(response.text)
            self.logger.info(response)
            self.logger.info(f"Image Posted > Lot Number: {record['lot']}. {response.text}")
        except Exception as ex:
            print(ex)
            self.logger.error(ex)
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
                r = requests.post('http://sdb.vindb.org:8080/input/postdata.php', data = {'data' : payload}, timeout=60)

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
       
        print('Data Pushed.... IAAI finished')

    def get_driver_chrome(self,current_directory = os.path.join(os.getcwd(),'downloads')):
        options = webdriver.FirefoxOptions()
        # options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        driver = webdriver.Firefox(options=options)
        return driver
    def by_pass(self,driver):
        sleep(15)
        try:
            sitekey = driver.find_element(by.XPATH,"//form//div[@class='g-recaptcha'][@data-sitekey]").get_attribute("data-sitekey")
        except:
            sitekey = ""
        driver.switch_to.frame("main-iframe")
        answer_input = driver.find_element(by.XPATH,"//textarea[@name='g-recaptcha-response']")

        if answer_input and not sitekey:
            src = driver.find_element(by.XPATH,"//iframe[contains(@src, 'google.com/recaptcha/api2/anchor')]").get_attribute("src")
            sitekey = src.split("k=", 1)[1].split("&", 1)[0]

        if sitekey and answer_input:
            print("Detected as recaptcha_v2...")
            post_url= str(driver.page_source).split('"POST", "')[1].split('"')[0]
            print(post_url)
            answer = self.recaptcha_v2(sitekey, driver.current_url,self.api_key)
            if answer:
                print("Token:", answer)
                driver.execute_script("""document.getElementById("g-recaptcha-response").innerHTML = arguments[0]""", answer)
                scriptTxt = """
                    var xhr;
                if (window.XMLHttpRequest) {
                    xhr = new XMLHttpRequest;
                } else {
                    xhr = new ActiveXObject("Microsoft.XMLHTTP");
                }
                var msg = "g-recaptcha-response=" + arguments[0];
                xhr.open("POST", arguments[1], true);
                xhr.withCredentials = true;
                
                xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
                xhr.onreadystatechange = function(){
                    if (xhr.readyState == 4) {
                        if (xhr.status == 200) {
                        window.parent.location.reload(true);
                        } else {
                        window.parent.location.reload(true); 
                        }
                    }
                }
                xhr.send(msg);

                """
                driver.execute_script(scriptTxt,answer,post_url)
                print("Solved recaptcha_v2 successfully.")
            else:
                print("Failed")
            driver.switch_to.default_content()

    def getCookies(self):     
        driver = self.get_driver_chrome()  
        driver.get('https://www.iaai.com')
        sleep(180)
        try:
            driver.find_element(by.XPATH,"//button[@id='onetrust-accept-btn-handler']").click()
        except:
            pass
        if '_Incapsula_Resource?SWUDNSAI=' in driver.page_source:
            driver.save_screenshot('./captcha.png')
            print('1) Found Captcha')
            self.logger.info('1) Found Captcha')
            self.by_pass(driver)
            sleep(25)

        print("Do any thing ......")
        email = "iaai@vinaudit.com"
        password = "9qSaFcC"
        sleep(25)
        try:

            driver.find_element(by.XPATH,'//div[@id="loginRow"]//a[@aria-label="Log In"]').click()
        except:
            print('2) Found Captcha')
            self.logger.info('2) Found Captcha')
            self.by_pass(driver)
            sleep(25)
            driver.find_element(by.XPATH,'//div[@id="loginRow"]//a[@aria-label="Log In"]').click()
        
        sleep(15)
        
        if '_Incapsula_Resource?SWUDNSAI=' in driver.page_source:
            self.by_pass(driver)
            sleep(25)


        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        WebDriverWait(driver, 35).until(EC.presence_of_element_located((by.XPATH,'//input[@name="Input.Email"]')))
        driver.find_element(by.XPATH,'//input[@name="Input.Email"]').send_keys(email)
        driver.find_element(by.XPATH,'//input[@name="Input.Password"]').send_keys(password)
        driver.find_element(by.XPATH,'//button[@class="btn btn-lg btn-block btn-primary"]').click()
        sleep(25)
        print('Logged In')


        if '_Incapsula_Resource?SWUDNSAI=' in driver.page_source:
            print('3) Found Captcha')
            self.logger.info('3) Found Captcha')
            self.by_pass(driver)
            sleep(25)

        sleep(35)
        cookies = driver.get_cookies()
        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
        cookies = cookies_dict.copy()
        driver.close()
        return cookies
    
    def start_requests(self):
        self.logger.info(f"Start time: {datetime.now()}")
        cookies = self.getCookies()
        
        yield scrapy.Request(self.main_url,cookies=cookies,callback=self.pagination,dont_filter=True,meta = {'cookies':cookies})
    def pagination(self,response):
        meta = response.meta
        cookies = meta.get('cookies')
        if '_Incapsula_Resource?SWUDNSAI=' in response.text:
            print("Passing captcha ....")
            IF=response.xpath('//iframe/@src').get()
            url=self.main_url+IF
            response=requests.get(url)
            HTML=scrapy.Selector(text=response.text)
            sitekey=HTML.xpath('//div[@data-sitekey]/@data-sitekey').get() 
            blankurl = HTML.xpath("//iframe[@id = 'main-iframe']/@src").get()
            post_url=self.main_url+blankurl
            self.pass_captcha(url,post_url,sitekey,self.api_key)
            yield scrapy.Request(self.main_url,cookies=cookies,callback=self.pagination,dont_filter=True)
        else:
            totalcars = int(response.xpath("//div/lable[@id = 'headerTotalAmount']/text()").get().replace("Vehicles","").replace(',','').strip())
            url = 'https://www.iaai.com/Search'
            lim =((totalcars//500+1)//2+1)+1
            endlim = ((totalcars//500+1))+1
            print('pagesfirsthalf',lim)
            print('pagesfirsthalf',endlim)
            for i in range(1,endlim):

                payload = json.dumps({
                    "Searches": [
                        {
                        "Facets": [
                            {
                            "Group": "IsDemo",
                            "Value": "False"
                            }
                        ],
                        "FullSearch": None,
                        "LongRanges": None
                        }
                    ],
                    "ZipCode": "",
                    "miles": 0,
                    "PageSize": 500,
                    "CurrentPage": i,
                    "Sort": [
                        {
                        "IsGeoSort": False,
                        "SortField": "AuctionDateTime",
                        "IsDescending": False
                        }
                    ],
                    "SaleStatusFilters": [
                        {
                        "SaleStatus": 1,
                        "IsSelected": True
                        }
                    ],
                    "BidStatusFilters": [
                        {
                        "BidStatus": 6,
                        "IsSelected": True
                        }
                    ]
                    }
                )
                headers = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Content-Type': 'application/json',
                'Origin': 'https://www.iaai.com',
                'Referer': 'https://www.iaai.com/Search?url=0ArZ4Si9I0V8KK51vW5SV5Evlu%2ffPY5%2fGiZKOhluHFA%3d',
                'Sec-Ch-Ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
                }
                yield scrapy.Request(url,method = "POST",body = payload,headers =headers,cookies =cookies,callback = self.parsedata,dont_filter = True,meta = {'payload':payload,'headers':headers,'again':False})
                
    def parsedata(self,response):
        meta = response.meta
        payload = meta.get('payload')
        headers = meta.get('headers')
        again = meta.get('again')
        url = 'https://www.iaai.com/Search'
        if '_Incapsula_Resource?SWUDNSAI=' in response.text:
            print("Passing captcha ....")
            self.logger.info("Passing captcha ....")
            if self.setcookie == None:
                cookies = self.getCookies()
                self.setcookie = cookies
            else:
                cookies = self.setcookie.copy()
                if again:
                    cookies = self.getCookies()
                    self.setcookie = cookies
            yield scrapy.Request(url,method = "POST",body = payload,headers =headers,cookies =cookies,callback = self.parsedata,dont_filter = True,meta = {'payload':payload,'headers':headers,'again':True,"dont_proxy": True})

        items = {}
        stock = response.xpath("//span[@class = 'data-list__label' and contains(text(),'Stock')]/following-sibling::span/text()").extract()
        vin = response.xpath("//span[@class = 'data-list__label' and contains(text(),'VIN')]/following-sibling::span/@name").extract()
        for i,v in enumerate(vin):   
            items['Lot#'] = stock[i]
            items['Vin#'] = v
            yield items
            print(items)
    