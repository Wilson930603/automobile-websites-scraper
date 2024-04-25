import scrapy
import json
import scrapy
from datetime import datetime
import os
import sys
from tqdm import tqdm
import mysql.connector
from mysql.connector import errorcode
import time
class Iaai_CA_Spider(scrapy.Spider):
    name = "ca_iaai"

    base_url = "https://ca.iaai.com"
    start_url = "https://ca.iaai.com/Auctions/Auctions"
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f'./{name}_logs'):
        os.makedirs(f'./{name}_logs')
    custom_settings = {
        'LOG_FILE': f'./{name}_logs/{name}_{current_date}.log',
        'ITEM_PIPELINES': {
            'automobile.pipelines.AutomobilePipeline': 300,
        },
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/105.0.0.0 Safari/537.36 OPR/91.0.4516.95',
        'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,'
                'application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip',
        # 'Referer': 'https://www.google.com/',
        'Upgrade-Insecure-Requests': '1'
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
        self.conn.commit()
        self.conn.close()
        os.remove(file_name)
        print(f'ca_iaai finished')    
    def start_requests(self):
        yield scrapy.Request(self.start_url,callback=self.location_page,headers=self.headers)

    
    def location_page(self,response):
        scirpt = response.xpath('//script[@id="AuctionListVM"]/text()').get().strip()
        
        url = "https://ca.iaai.com/Search/GetSearchResult/"

        
        headers = {
        'Content-Type': 'application/json',
        }
        data = json.loads(scirpt)
        for listing_ids in data["AuctionListWeekView"]:
            for ids in listing_ids["AuctionList"]:
                print(f"{ids.get('AuctionID','NA')}")
                id = ids.get('AuctionID','NA')
                if id is not "NA":
                    custom_payload = {
                        "PageLoading": False,
                        "IsKeywordSearch": False,
                        "IsNewSearch": True,
                        "RunlistSort": "",
                        "IgnoreKeywordYear": False,
                        "YearFrom": "",
                        "YearTo": "",
                        "VehicleAge": "-1",
                        "AuctionToday": False,
                        "AuctionTomorrow": False,
                        "VehicleBrand": "",
                        "StockFilter": "",
                        "IsBuyNow": False,
                        "IsVehicleRemarketing": False,
                        "ShowOnlyPublicAuctions": False,
                        "VehicleTypeIds": "",
                        "VehicleMakeIds": "",
                        "VehicleModelIds": "",
                        "AuctionTypeIds": "",
                        "AuctionIds": f"1-{id}~",
                        "RequestSource": "",
                        "IsManagerPick": False,
                        "IsWatching": False,
                        "IsPreBidWinning": False,
                        "IsPreBidOutBid": False,
                        "AuctionMode": "",
                        "IsSiteSale": "",
                        "IsBuyNowOffer": False,
                        "VehicleTypeListPageIndex": 1,
                        "BranchListPageIndex": 1,
                        "MakeListPageIndex": 1,
                        "ModelListPageIndex": 1,
                        "RunlistPageIndex": 1,
                        "AttributePageSize": 100,
                        "Caller": "start",
                        "RunlistPageSize": "10"
                    }
                    payload = json.dumps(custom_payload)
                    yield scrapy.Request(url,callback=self.listing_page,headers=headers,method="POST",body=payload,meta={"payload":custom_payload})
    
    def listing_page(self,response):
        vec_page = "https://ca.iaai.com/Vehicles/VehicleDetails?itemid="
        meta = response.meta
        custom_payload = meta.get('payload')
        listing_data = json.loads(response.text)
        custom_payload["RunlistPageIndex"] +=1
        run_list = listing_data["RunList"]
        url = "https://ca.iaai.com/Search/GetSearchResult/"
        headers = {
        'Content-Type': 'application/json',
        }
        for vec_ids in run_list:
            id = vec_ids.get('StockId')
            if id is not None:
                yield scrapy.Request(vec_page+str(id),callback=self.information,headers=self.headers)
        if len(run_list)==10:
            custom_payload
            payload = json.dumps(custom_payload)
            yield scrapy.Request(url,callback=self.listing_page,headers=headers,method="POST",body=payload,meta={"payload":custom_payload})

    
    def information(self,response):

        vm_data = response.xpath('//script[@id="VehicleDetailsVM"]/text()').get().strip()
        vm_data = json.loads(vm_data)
        vin = vm_data.get('VIN','NA')
        lot = vm_data.get('StockNo','NA')
        date = f"{vm_data.get('Day')} {vm_data.get('Date')}, {vm_data.get('Month')} - {vm_data.get('Time')}"
        selling_location = f"{vm_data.get('LocationName')}, {vm_data.get('City')}, {vm_data.get('Province')}"
        current_bid = vm_data.get('HighPrebid','NA')
        title = "{} {} {}".format(vm_data.get('Year',''),vm_data.get('Make'),vm_data.get('Model'))
        model = vm_data.get('Model','NA')
        conditioning = vm_data.get('ConditionInfo',[])
        odo_meter = 'NA'
        airbags = 'NA'
        key = 'NA'
        for x in conditioning:
            if x.get('DisplayText') == 'Odometer':
                if len(x['DisplayValues'])>0:
                    odo_meter = x['DisplayValues'][0].get('Text')
            elif x.get('DisplayText') == 'AirBags':
                if len(x['DisplayValues'])>0:
                    airbags = ', '.join([f"{airBag_info.get('Label')}:{airBag_info.get('Text')}" for airBag_info in x['DisplayValues']])
            elif x.get('Name') == 'KeysPresent':
                if len(x['DisplayValues'])>0:
                    key = x['DisplayValues'][0].get('Text')
        overview = vm_data.get('OverviewExtendedInfo',[])
        primary_damage = 'NA'
        secondary_damage = 'NA'
        for x in overview:
            if x.get('DisplayText') == 'Primary Damage':
                if len(x['DisplayValues'])>0:
                    primary_damage = x['DisplayValues'][0].get('Text')
            elif x.get('DisplayText') == 'Secondary Damage':
                if len(x['DisplayValues'])>0:
                    secondary_damage = x['DisplayValues'][0].get('Text')
        images = ', '.join([img.get('PictureUrl') for img in vm_data.get('VehicleImages',[])])
        fuel_type = vm_data.get('FuleType','')

        vin_info = vm_data.get('VINInfo',[])
        engine = 'NA'
        color = 'NA'
        drive_type= 'NA'
        for x in vin_info:
            if x.get('DisplayText') == 'Engine':
                if len(x['DisplayValues'])>0:
                    engine = x['DisplayValues'][0].get('Text')
            elif x.get('DisplayText') == 'Exterior Colour':
                if len(x['DisplayValues'])>0:
                    color = x['DisplayValues'][0].get('Text')
            elif x.get('DisplayText') == 'Transmission':
                if len(x['DisplayValues'])>0:
                    drive_type = x['DisplayValues'][0].get('Text')
                
        
        overview_info = vm_data.get('OverviewInfo',[])
        cylinders = 'NA'
        retail_value = 'NA'
        body_style = 'NA'
        rundrive = 'NA'
        if len(overview_info)>0:
            try:
                body_style = overview_info[0]['DisplayValues'][0].get('Text')
            except:
                body_style= 'NA'
            for x in overview_info:
                if x.get('Name') == 'OverviewCYL':
                    if len(x['DisplayValues'])>0:
                        cylinders = x['DisplayValues'][0].get('Text')
                elif x.get('Name') == 'OverviewActualCashValue':
                    if len(x['DisplayValues'])>0:
                        retail_value = x['DisplayValues'][0].get('Text')
                elif x.get('Name') == 'OverviewRunDrive':
                    if len(x['DisplayValues'])>0:
                        rundrive = x['DisplayValues'][0].get('Text')
                

        items = {}
        items["vin"] = vin
        items["source"] = self.name
        items["date"] = self.current_date
        items["type"] = "salvage"
        items["Lot#"] = lot
        items["Sale Date"] = date
        items["Sale Location"] = selling_location
        items["Listing URL"] = response.url
        items["Sale Status"] = ''
        items["Current Bid"] = current_bid
        items["Title Code"] = title
        items["Odometer"] = odo_meter
        items["Highlights"] = ''
        items["model"] = model
        items["Primary Damage"] = primary_damage
        items["Secondary Damage"] = secondary_damage
        items["Cylinders"] = cylinders
        items["Exterior/Interior"] = ''
        items["Airbags"] = airbags
        items["Notes"] = vm_data.get('UserVehicleNotes','NA')
        items["Drive"] = drive_type
        items["Documents Type"] = ''
        items["Image URLs"] = images
        items["Fuel"] = fuel_type
        items["Est. Retail Value"] = retail_value
        items["Lane/Item/Grid/Row"] = ''
        items["Keys"] = key
        items["Engine Type"] = engine
        items["Color"] = color  
        items["Body Style"] = body_style
        items["OverviewRunDrive"] = rundrive
        items["Item ID"] = vm_data.get('ItemId')
        yield items
                

