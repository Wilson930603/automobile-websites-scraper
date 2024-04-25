import scrapy,json
from datetime import datetime
import os
import sys
from time import sleep
from tqdm import tqdm
import mysql.connector 
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By as by
from selenium import webdriver
import pandas as pd
import requests
from io import BytesIO
import mysql.connector
from twocaptcha import TwoCaptcha
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
    api_key='d368d93ac4830dc4a52b5351a7b92626'
    name = 'copart_fullVIN'
    cookies = {'incap_ses_1129_242093': ''}
    csv_filename = f'./Images_sdb_lots/{name}_lots.csv'
    data_lock = threading.Lock()
    available_lots = pd.read_csv(csv_filename,low_memory=False)["lots"].astype(str).tolist()

    headers_json = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0','Accept': 'application/json, text/plain, */*','Accept-Language': 'en-GB,en;q=0.5','X-Requested-With': 'XMLHttpRequest','Access-Control-Allow-Headers': 'Content-Type, X-XSRF-TOKEN','Content-Type': 'application/json','Origin': 'https://www.copart.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
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
            if "*" in str(item["VIN"]):
                return None

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
            print('Exporting data from db for copart')
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
                    print("Copart_ batch download: {}".format(len(lot_type)))
                    self.logger.info("Copart_ batch download: {}".format(len(lot_type)))
                final_data = lot_type

            print("Data exported to JSON successfully.")
            self.logger.info("Data exported to JSON successfully.")
            return final_data
        except Exception as ex:
            print("Error exporting data: {}".format(ex))
    def append_lot_to_csv(self,lot):
        # Append a single lot to the CSV file
        with open(self.csv_filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([lot])
    def post_images(self,record):
        #for record in records:
        try:
            self.logger.info(record)
            images = record["Image URLs"].split(' , ')
            if len(images) == 1:
                images = record["Image URLs"].split(', ')
            
            data = {
                'vehicle': json.dumps(record),
            }
            if len(images)>0:
                for itr,img in enumerate(images):
                    response = requests.get(url=img,headers=self.headers)
                    image_data = BytesIO(response.content)
                    self.logger.info(f'Lot Number: {record["lot"]} Downoaded Image: {img}')
                    data[f"data_files_r{itr}"] = (f"image_{record['lot']}_{itr + 1}.jpg",image_data,'image/jpeg')
                    
            response = requests.post('http://sdb.vindb.org/api/input/multipart-form-data/singular', files=data)
            with self.data_lock:
                if record['lot'] not in self.available_lots:
                    self.append_lot_to_csv(record['lot'])
                    self.logger.info(f"{record['lot']} added to available data")
            self.logger.info(f"Image Posted > Lot Number: {record['lot']}. {response.text}")
        except Exception as ex:
            print(f"Main Exception {ex}")
            self.logger.error(f"{record['lot']}, {record['Image URLs']}: Main Exception {ex}")
    def upload_images_parallel(self, records):
        threads = []
        for record in records:
            threads.append(threading.Thread(target=self.post_images,args=(record,)))
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
    
    def closed(self,reason):
        saved_file = sys.argv[-1]
        current_directory = os.getcwd()
        DATABASE_NAME='intermediate_db'
        HOST='104.238.228.196'
        username='root'
        password='Crawler@2021'
        cnx = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=username,password=password,charset='utf8')

        tables = ['listing_records']

        interdb_records = self.export_table_to_json_batch(cnx,tables[0],'listing_records_copart.json',"copart_salvage")
        cnx.close()
        formated_data = []
        if len(interdb_records)>0:
            print('Copart Formating Data')
            df = pd.read_csv("{}".format(saved_file),low_memory=False)
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
        batch_size = 20
        print(f'Pushing Records To Database... Total Records to push: {len(formated_data)}')
        for i in tqdm(range(0, len(formated_data), batch_size)):
            batch = formated_data[i:i+batch_size]
            self.upload_images_parallel(batch)
            self.logger.info(f'Inserted batch {i // batch_size + 1}/{len(formated_data) // batch_size}')
        os.remove(saved_file)
        print('Data Pushed.... copart finished')
        self.logger.info('Data Pushed.... copart finished')
    
    def recaptcha_v2(self,sitekey, url,api_key):
        try:
            solver = TwoCaptcha(api_key)
            result = solver.solve_captcha(site_key=sitekey, page_url=url)
            # result = solver.recaptcha(sitekey=sitekey, url=url)
        except Exception as e:
            # print(e)
            result = ""
        return result
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

    def login(self,driver):
    
        print('Copart Loggin In')
        # email = "copart@autoscale.ventures"
        # password = "r)KJpwW-fkS=1"
        email = "copart@vinaudit.com"
        password = "JTkK7#a5zKbM"

        driver.get('https://www.copart.com/login/')
        sleep(20)
        # input('Here')
        if '_Incapsula_Resource?SWUDNSAI=' in driver.page_source:
            # driver.save_screenshot('./captcha.png')
            print('1) Found Captcha')
            self.logger.info('1) Found Captcha')
            self.by_pass(driver)
            sleep(20)
        driver.find_element(by.XPATH,'//input[@data-uname="loginUsernametextbox"]').send_keys(email)
        driver.find_element(by.XPATH,'//input[@data-uname="loginPasswordtextbox"]').send_keys(password)

        driver.find_element(by.XPATH,'//button[@data-uname="loginSigninmemberbutton"]').click()
        sleep(10)
        print('Copart Loggin In complete')
        try:

            driver.find_element(by.XPATH,'//div[@id="memberTextSubscriptionModal"]//button[@class="close"]').click()
        except:
            pass
        sleep(5)
        driver.find_element(by.XPATH,'//button[@data-uname="homepageHeadersearchsubmit"]').click()
        sleep(5)
        cookies = driver.get_cookies()
        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
        cookies = cookies_dict.copy()
        return cookies
    def start_requests(self):
        driver = get_driver_firefox()
        self.cookies = self.login(driver)
        driver.close()

        json_data = {'query': ['*',],'filter': {},'sort': ['auction_date_type desc','auction_date_utc asc',],'page': 0,'size': 100,'start': 0,'watchListOnly': False,'freeFormSearch': False,'hideImages': False,'defaultSort': False,'specificRowProvided': False,'displayName': '','searchName': '','backUrl': '','includeTagByField': {},'rawParams': {}}
        url='https://www.copart.com/public/lots/search-results'
        yield scrapy.Request(url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})
    
    def parse_data(self,response):
        json_data=response.meta['json_data']
        Data=json.loads(response.text)
        for i,row in enumerate(Data['data']['results']['content']):
            lotno = row.get('ln','')
            myurl = f"https://www.copart.com/public/data/lotdetails/solr/{lotno}"
            vin = row.get('fv','')
            lotno = row.get('ln','')

            url = f"https://www.copart.com/lot/{lotno}/{row.get('ldu','')}"
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


        

