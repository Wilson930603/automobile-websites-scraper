import scrapy,json,os, sys,copy
from datetime import datetime
from selenium import webdriver
from tqdm import tqdm
import mysql.connector
from webdriver_manager.firefox import GeckoDriverManager
import csv
import threading
def get_driver_firefox():
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

    driver = webdriver.Firefox(executable_path = GeckoDriverManager().install(), options=options)
    # driver = webdriver.Firefox(executable_path = './root/geckodriver', options=options)
    
    return driver
class CrawlerSpider(scrapy.Spider):
    download_delay = 0.3
    # concurrent_request = 10
    name = 'copart'
    interdb_records = {}
    cookies = {'incap_ses_1129_242093': ''}
    headers_json = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0','Accept': 'application/json, text/plain, */*','Accept-Language': 'en-GB,en;q=0.5','X-Requested-With': 'XMLHttpRequest','Access-Control-Allow-Headers': 'Content-Type, X-XSRF-TOKEN','Content-Type': 'application/json','Origin': 'https://www.copart.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
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
    img_file_name = f"./datafolder/copart_images_{current_date}.csv"
    csv_items = {"img_url":[]}
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
            # return final_data
            self.interdb_records = final_data
        except Exception as ex:
            print("Error exporting data: {}".format(ex))
    
    def return_formated_data(self,item):
        vin = item.get('vin', '')
        document_type = item.get('type', '')
        sale_date = item.get('date', '')
        source = item.get('source','')
        lot = item.get('lot')
        details = ''
        for key, value in item.items():
            if key not in ['vin', 'type', 'date','source','lot']:
                details += f"{key}: {value}\n"
        ITEM = {
            "vin":vin,
            "Type":document_type,
            "Date":sale_date,
            "Source":source,
            "Lot":lot,
            "Details":details}
        return ITEM
    def matched_products(self,item,old_data):
        # Split the string into lines
        new_item = copy.deepcopy(item)
        new_old = copy.deepcopy(old_data)
        lines = new_old["Details"].split('\n')
        # Extract keys and values from the lines and create a dictionary
        data_dict_old = {line.split(': ')[0]: line.split(': ')[1] for line in lines if ': ' in line}
        data_dict_old["vin"] = new_old["Vin"]
        data_dict_old["type"] = new_old["Type"]
        # data_dict_old["date"] = new_old["Date"]
        data_dict_old["source"] = new_old["Source"]
        data_dict_old["lot"] = new_old["Lot"]


        lines = new_item["Details"].split('\n')
        # Extract keys and values from the lines and create a dictionary
        data_dict_new = {line.split(': ')[0]: line.split(': ')[1] for line in lines if ': ' in line}
        data_dict_new["vin"] = new_item["vin"]
        data_dict_new["type"] = new_item["Type"]
        # data_dict_new["date"] = new_item["Date"]
        data_dict_new["source"] = new_item["Source"]
        data_dict_new["lot"] = str(new_item["Lot"])

        del data_dict_old["Image URLs"]
        del data_dict_new["Image URLs"]
        data_dict_new = self.return_formated_data(data_dict_new)
        data_dict_old = self.return_formated_data(data_dict_old)
        if data_dict_new == data_dict_old:
            return True


        return False
    def closed(self,reason):
        # Open the CSV file in write mode
        file_name = sys.argv[-1]
        # file_name = './datafolder/copart_2023-10-17.json'
        with open(file_name, 'r') as file:
            data = json.load(file)
        
        if data:
            # interdb_records = self.export_table_to_json_batch(cnx,'listing_records','listing_records_copart.json',"copart")
            
            self.db_thread.join()

            img_url = "https://www.copart.com/public/data/lotdetails/solr/lotImages/{lotno}/USA"
            print("Checking New or needs updating records")
            for item in tqdm(data):
                if self.interdb_records.get(str(item["Lot"])):
                    if not self.matched_products(item,self.interdb_records[str(item["Lot"])]):
                        self.csv_items["img_url"].append(img_url.format(lotno=str(item["Lot"])))
                else:
                    self.csv_items["img_url"].append(img_url.format(lotno=str(item["Lot"])))

                        
            
            with open(self.img_file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                
                # Write the header row (column names)
                writer.writerow(self.csv_items.keys())
                
                # Write the data
                for row in zip(*self.csv_items.values()):
                    writer.writerow(row)
            # os.remove(file_name)
        else:
            print('Error while extraing data from json file')
            self.logger.error('Error while extracting data from json file')
        print('Copart Finished')  
    def start_requests(self):
        # yield scrapy.Request("https://www.google.com",callback=self.testing)
        json_data = {'query': ['*',],'filter': {},'sort': ['auction_date_type desc','auction_date_utc asc',],'page': 0,'size': 100,'start': 0,'watchListOnly': False,'freeFormSearch': False,'hideImages': False,'defaultSort': False,'specificRowProvided': False,'displayName': '','searchName': '','backUrl': '','includeTagByField': {},'rawParams': {}}
        url='https://www.copart.com/public/lots/search-results'
        yield scrapy.Request(url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})
        DATABASE_NAME='intermediate_db'
        HOST='104.238.228.196'
        username='root'
        password='Crawler@2021'
        cnx = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=username,password=password,charset='utf8')
        self.db_thread = threading.Thread(target=self.export_table_to_json_batch,args=(cnx,'listing_records','listing_records_copart.json','copart_salvage'))
        self.db_thread.start()
    def testing(self,response):
        print('Testing')
    def parse_data(self,response):
        
        json_data=response.meta['json_data']
        Data=json.loads(response.text)
        self.logger.info(f"Total listing length: {len(Data['data']['results']['content'])}")
        print(f"Total listing length: {len(Data['data']['results']['content'])}")
        for i,row in enumerate(Data['data']['results']['content']):
            # print('\n ------------')
            # print(row)
            # Do any thing do you want
            lotno = row.get('ln','')
            myurl = f"https://www.copart.com/public/data/lotdetails/solr/{lotno}"
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
            # if self.full_images.get(lotno):
            #     images = self.full_images[lotno]
            #     print('FOUND IMAGE')
            #     self.logger.info('Found full images')
            # else:
            #     self.logger.info('Not Found full images')
            url = f"https://www.copart.com/lot/{lotno}/{row.get('ldu','')}"

            print(vin)
            current_date = datetime.now().strftime("%Y-%m-%d")
            items = {}
            items["vin"] = vin
            items["source"] = 'copart_salvage'
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
            img_url = f"https://www.copart.com/public/data/lotdetails/solr/lotImages/{lotno}/USA"
            # self.csv_items["img_url"].append(img_url)
            yield items
            
        if len(Data['data']['results']['content'])>0:
            json_data['page']+=1
            json_data['start']=json_data['page']*json_data['size']
            yield scrapy.Request(response.url,callback=self.parse_data,cookies=self.cookies,headers=self.headers_json,method='POST',body=json.dumps(json_data),meta={'json_data':json_data})

