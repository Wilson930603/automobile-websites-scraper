import sys
from urllib.parse import urljoin
import scrapy,json,re,os,platform,requests
from tqdm import tqdm
import mysql.connector 
from mysql.connector import errorcode
from datetime import datetime
import time
email = 'iaai@vinaudit.com'
password = '9qSaFcC'


class CrawlerSpider(scrapy.Spider):
    try:
        pageno = sys.argv[-1]
        pageno = int(pageno.split('=')[-1])
        print(f"StartPageEntered = {pageno}")
    except:
        pageno = 0
    name = 'iaai'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
    if not os.path.exists(f'./{name}_logs'):
        os.makedirs(f'./{name}_logs')
    # CONCURRENT_REQUESTS
    concurrent_requests = 100
    headers = {
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
            #             'Chrome/105.0.0.0 Safari/537.36 OPR/91.0.4516.95',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,'
                    'application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip',
            'Upgrade-Insecure-Requests': '1'
        }
    custom_settings = {
        'ROTATING_PROXY_LIST_PATH': 'proxies.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': concurrent_requests,
        'CONCURRENT_REQUESTS_PER_IP':1,
        'ITEM_PIPELINES': {
            'automobile.pipelines.AutomobilePipeline': 300,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        },
        'LOG_FILE': f'./{name}_logs/{name}_{current_date}_page_{pageno}.log'
    }
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'

    main_url = "https://www.iaai.com/Search"
    baseUrl = "https://www.iaai.com"

    api_key='d368d93ac4830dc4a52b5351a7b92626'
    cookies = {'visid_incap_2807936': 'QfsahuVCQ7a9M4lKKqXtW+X81GQAAAAAQkIPAAAAAACAhjeuAWjPmxb+i2ZtImAKAB/hYd9AQRX+','incap_ses_225_2807936': 'YnfkCdhDjixTaZ+D4VwfA+X81GQAAAAAdSDMyf11CalgBXRzNbJqXg==','_ga_8J4GTR5B9Q': 'GS1.1.1691683545.2.0.1691683545.60.0.0','nlbi_2807936': 'vUNxZom9eDah0DUxxRLPjgAAAAAg6wUhvig9HdQXS7hHpDEw','nlbi_2807936_2147483392': 'K1NhZwpceWsqZFTkxRLPjgAAAACAH3BpwqqpZj+vGcuZmXlV','reese84': '3:lPStjXfxYTLMQYyZjYhTUw==:TpcyP/pJCT5FTw8bpxbJftP/2eQlEoQJsgBLppfMiMyxlXGIcbgeZwuX/KwuPkVDsYUnEZ8HqVk0bHPP1uDvdyx5a5DP91K5qxhYEpOZJNFYJg1rgj3sS+GQO06vhsrGpJyT4fVpIpsV6q6w0lE8b5LUmRYJc+QC25fskayM8rEFS3POk8CjWMzPbKst0WhjbOXQooRmhtnAbnyVy6MMpnXcb2A0ujki5RlPMms+5tCYMOsiFH6C2O/HT/DKyb8+UUCbZm/hP9W9NQ1OR75GPaCk8Nv6P2l22YMMGlegd7uiO/1Qc0i/BIoNCQLXikjyIw2qRJYY88kNoWhzj074WslcboNbTA08v2VTdl5VEWLcvtnibHXy6P4aIFenPjaDVlAWJKomXqBWEOqWIGScQQw/yInHbTz8ZJR7W3WLm5soFgzP2bg4kTnYUELaPzWD+JM666Atqa6P9hzsfQdO3iFV6oeFfR4EQywHK1uBVCjKBRKLXFIAfgpXouf8wF84:c3Pp0vB9HnBmKyXAwg9C6a7/soDdDuAs/c+U6iPBELo=','incap_sh_2807936': 'IAvVZAAAAACg/65ZDAAIoJbUpgYQ2JXUpgb4OMxi+fZ1+UkEL4q3H2Z2','X-Forwarded-For_IpAddress': '104.207.155.22^%^2C^%^20198.143.33.4^%^3A35098','BrokerPopupIPAddress': '1758436118-104.207.155.22-1758436118','BrokerPopupCountryCode': 'NotFound','IAAITrackingCookie': '94be7dd8-532a-412c-9892-6466c4a52365','_sfid_4446': '{^%^22anonymousId^%^22:^%^22eeec07cfa26ac83b^%^22^%^2C^%^22consents^%^22:^[^]}','_evga_4ff5': '{^%^22uuid^%^22:^%^22eeec07cfa26ac83b^%^22}','TimeZoneMapID': '120','mdLogger': 'false','kampyle_userid': 'a4a4-be0f-6d8d-503f-0c0e-a17f-cee5-4694','kampyleUserSession': '1691681659597','kampyleSessionPageCounter': '1','kampyleUserSessionsCount': '1','OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Aug+10+2023+15^%^3A34^%^3A22+GMT^%^2B0000+(Coordinated+Universal+Time)&version=202306.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=https^%^3A^%^2F^%^2Fwww.iaai.com^%^2F&groups=C0001^%^3A1^%^2CC0003^%^3A1^%^2CC0002^%^3A1^%^2CC0004^%^3A1','actualOptanonConsent': '^%^2CC0001^%^2CC0003^%^2CC0002^%^2CC0004^%^2C','_gcl_au': '1.1.820744503.1691681661','_ga': 'GA1.2.772816893.1691681662','_uetsid': '619eb0e0379311ee993cd9dc7cd9dfc8','_uetvid': '619ed5f0379311eebe1e6fa219c15d3b','_gid': 'GA1.2.2003766068.1691681663','ln_or': 'eyIyMzg4ODk3IjoiZCJ9','_fbp': 'fb.1.1691681664609.1972545375','_clck': '1wec7y^|2^|fe1^|0^|1317','_clsk': '12wiohg^|1691681665405^|1^|0^|i.clarity.ms/collect'}
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
        file_name = sys.argv[-3]
        file = sys.argv[-3]
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
        print(f'iaai finished')    
    def start_requests(self):
        pgno = self.pageno
        yield scrapy.Request(
            self.main_url,
            callback=self.pagination,
            dont_filter=True,
            headers=self.headers,
            meta = {
                'pgno':pgno
                }
            )
    def pagination(self,response):
        meta = response.meta
        cookies = meta.get('cookies')
        pgno = meta.get('pgno')
        if '_Incapsula_Resource?SWUDNSAI=' in response.text:
            print("Pagination: Passing captcha ....")
            self.logger.error("Pagination: Passing captcha ....")
            
            yield scrapy.Request(
                self.main_url,
                callback=self.pagination,
                dont_filter=True,
                headers=self.headers,
                meta = {
                'pgno':pgno
                }
                
                )
        else:
            totalcars = int(response.xpath("//div/lable[@id = 'headerTotalAmount']/text()").get().replace("Vehicles","").replace(',','').strip())
            url = 'https://www.iaai.com/Search'
            endlim = ((totalcars//500+1))+1
            print('pagesfirsthalf',endlim)
            for i in range(pgno,endlim,10):

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
})
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
                # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
                }
                
                
                

            
                yield scrapy.Request(
                    url,
                    method = "POST",
                    body = payload,
                    # cookies=self.cookies,
                    headers = headers,
                    callback=self.getListings,
                    meta={"payload":payload,"page":i})


    def getListings(self,response):
        meta = response.meta
        payload = meta.get('payload')
        page = meta.get('page')
        if '_Incapsula_Resource?SWUDNSAI=' in response.text:
            print(f"getListing: Passing captcha .... page: {page}")
            self.logger.error(f"getListing: Passing captcha .... page: {page}")
            yield scrapy.Request(
                self.main_url,
                method = "POST",
                body = payload,
                headers=self.headers,
                callback=self.getListings,
                dont_filter=True,
                meta={"payload":payload,"page":page}
                )
        else:
            urls = [urljoin("https://www.iaai.com/",u) for u in response.xpath("//h4[@class = 'heading-7 rtl-disabled']/a/@href").extract()]
            for link in urls:
                yield scrapy.Request(
                    link,
                    callback = self.information_page,
                    headers=self.headers,
                    dont_filter=True,
                )
        
    def information_page(self,response):
        if '_Incapsula_Resource?SWUDNSAI=' in response.text:
            print("Information page: Passing captcha ....")
            self.logger.error("Information page: Passing captcha ....")
            yield scrapy.Request(
                response.url,
                headers=self.headers,
                callback=self.information_page,
                dont_filter=True,
                )
        else:
            title = response.xpath('//h1[contains(@class,"heading-2")]/text()').get(default='NA')

            lot = response.xpath("//li[contains(@class, 'data-list__item')]//span[@class='data-list__value text-bold' and preceding-sibling::span[@class='data-list__label' and contains(text(), 'Stock #:')]]/text()").get(default='NA')
            vin = response.xpath('//span[text()="VIN (Status):"]/following-sibling::span[@class="data-list__value hidden-print text-bold"]/span[2]/text()').get(default='NA').strip()
            date = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Auction Date and Time:']/following-sibling::a/text()").get(default='NA').replace('\r\n','').replace(' ','').strip()
            selling_location = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Selling Branch:']/following-sibling::span/a/text()").get(default='NA').strip()
            current_bid = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Current Bid:']/following-sibling::span/text()").get(default='NA').strip()
            odo_meter = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Odometer:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            primary_damage = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Primary Damage:']/following-sibling::span[@class='data-list__value'][2]/text()").get(default='NA').strip()
            secondary_damage = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Secondary Damage:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            cylinders = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Cylinders:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            drive_type = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Drive Line Type:']/following-sibling::span[@class='data-list__value']/text()").get(default = 'NA').strip()
            images = ' , '.join(response.xpath("//div[@class='vehicle-image__thumb-container']/img").extract())
            fuel_type = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Fuel Type:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            retail_value = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Actual Cash Value:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            key = response.xpath("//li[contains(@class, 'data-list__item')]//span[@class='data-list__label hidden-print' and text()='Key:']/following-sibling::span[@class='data-list__value' and @id='key_image_div']/span[@class='link link-tooltip']/text()[2]").get(default='NA').strip()
            engine_type = response.xpath("//li[contains(@class, 'data-list__item')]//span[@class='data-list__label' and text()='Engine:']/following-sibling::span[@class='data-list__value' and @id='engine_image_div']/span[@class='link' and @data-toggle='modal']/span[@class='icon icon-key']/following-sibling::text()").get(default='NA').strip()
            paint = response.xpath("//div[@class='panel-body']//div[@class='chrome-list']//h6[@class='label' and text()='PAINT']/following-sibling::ul/li/text()").get(default = 'NA').strip()
            body_style = response.xpath("//li[contains(@class, 'data-list__item')]//span[text()='Body Style:']/following-sibling::span[@class='data-list__value']/text()").get(default='NA').strip()
            data = response.xpath('//script[@id="ProductDetailsVM"]/text()').get()
            
            doc = response.xpath("//li/span[contains(text(),'Title/Sale Doc:')]/following-sibling::span/text()").get(default = 'NA').strip()
            airbags = response.xpath("//li/span[contains(text(),'Airbags')]/following-sibling::span/text()").get(default = 'NA').strip()
            model = response.xpath("//li/span[contains(text(),'Model')]/following-sibling::span/text()").get(default = 'NA').strip()
            intext = response.xpath("//li/span[contains(text(),'Exterior/Interior:')]/following-sibling::span/text()").get(default = 'NA').strip()
            xdata = json.loads(data)
            try:
                img_data = []
                set_at_begin = 'https://vis.iaai.com/resizer?imageKeys='
                set_at_end = '&width=161&height=120'
                for img in xdata['inventoryView']['imageDimensions']['keys']['$values']:
                    img_data.append(set_at_begin+img.get('k')+set_at_end)
                images = ' , '.join(img_data)
            except Exception as ex:
                images = 'NA'
            
            items = {}
            items["vin"] = vin
            items["source"] = self.name
            items["date"] = self.DATE_CRAWL
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
            items["Exterior/Interior"] = intext
            items["Airbags"] = airbags
            items["Notes"] = ''
            items["Drive"] = drive_type
            items["Documents Type"] = doc
            items["Image URLs"] = images
            items["Fuel"] = fuel_type
            items["Est. Retail Value"] = retail_value
            items["Lane/Item/Grid/Row"] = ''
            items["Keys"] = key
            items["Engine Type"] = engine_type
            items["Color"] = paint  
            items["Body Style"] = body_style
            yield items

            print(f'Success - {response.url}')
