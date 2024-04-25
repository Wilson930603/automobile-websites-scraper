# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from mysql.connector import Error
import mysql.connector,re
import requests
import ujson

class AutomobilePipeline:
    def open_spider(self,spider):
        self.DATABASE_NAME='intermediate_db'    
        self.HOST='104.238.228.196'
        self.username='root'
        self.password='Crawler@2021'
        # self.DATABASE_NAME='crawler'    
        # self.HOST='localhost'
        # self.username='root'
        # self.password=''
        try:
            self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
            if self.conn.is_connected():
                print('Connected to DB')
                db_Info = self.conn.get_server_info()
                print(f"Connected to MySQL Server version {db_Info}")
                self.create_table()
                spider.conn = self.conn
            else:
                print('Not connect to DB')
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")

            self.conn=None
    def process_item(self, item, spider):

        # success_check = self.post_records([item])
        # self.push_to_mysql(item)
        item = self.return_formated_data(item)
        # print(success_check)
        return item
    def return_formated_data(self,item):
        vin = item.get('vin', '')
        document_type = item.get('type', '')
        sale_date = item.get('date', '')
        source = item.get('source','')
        lot = item.get('Lot#')
        details = ''
        for key, value in item.items():
            if key not in ['vin', 'type', 'date','source','Lot#']:
                details += f"{key}: {value}\n"
        ITEM = {
            "vin":vin,
            "Type":document_type,
            "Date":sale_date,
            "Source":source,
            "Lot":lot,
            "Details":details}
        return ITEM
    def already_available(self,vin):
        sql = f"Select id from listing_records Where Vin = '{vin}'"
        cursor = self.conn.cursor()
        mycursor = self.conn.cursor(buffered=True)
        mycursor.execute(sql)
        result = mycursor.fetchone()
        mycursor.close()

        if result:
            return result[0]
        return None
    def push_to_mysql(self,data):
        if isinstance(data, dict):  # Single dictionary
            data = [data]

        cursor = self.conn.cursor()
        for item in data:
            vin = item.get('vin', '')
            id = self.already_available(vin)
            if id:
                document_type = item.get('type', '')
                sale_date = item.get('date', '')
                source = item.get('source','')

                details = ''
                for key, value in item.items():
                    if key not in ['vin', 'type', 'date','source']:
                        details += f"{key}: {value}\n"

                query = "UPDATE listing_records SET Type = %s, Date = %s, Source = %s, Details = %s WHERE ID = %s"
                values = (document_type, sale_date, source, details, id)
                cursor.execute(query, values)
                print('Record Updated')
            else:

                document_type = item.get('type', '')
                sale_date = item.get('date', '')
                source = item.get('source','')
                
                details = ''
                for key, value in item.items():
                    if key not in ['vin', 'type', 'date','source']:
                        details += f"{key}: {value}\n"
                
                query = "INSERT INTO listing_records (Vin, Type, Date, Source, Details) VALUES (%s, %s, %s, %s, %s)"
                values = (vin, document_type, sale_date, source, details)
                cursor.execute(query, values)
                print('Record posted')
        self.conn.commit()
        # input('One done')
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
                # r = requests.post('http://sdb.vindb.org/input/postdata.php', data = {'data' : payload}, timeout=60)
                r = requests.post('http://sdb-dev.vindb.org/input/postdata.php', data = {'data' : payload}, timeout=60)
                print(r.text)
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

        return ret, (total_count, valid_count, invalid_count)
    def create_table(self):
        sql = """
            CREATE TABLE IF NOT EXISTS listing_records (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                Vin VARCHAR(255),
                Type VARCHAR(255),
                Date DATE,
                Source VARCHAR(255),
                Lot VARCHAR(255),
                Details LONGTEXT,
                UNIQUE INDEX uc_vin_lot_source (Vin, Lot, Source)
            )
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()



