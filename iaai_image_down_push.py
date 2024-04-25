import mysql.connector 
from mysql.connector import errorcode
import time
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import logging, json, requests
from io import BytesIO
from random import randint
from fake_useragent import UserAgent
import re
import sys
import threading
import concurrent.futures
iaai_2_full_vin_csv = f"./datafolder/full_iaaiVin_28.csv"
# Define a lock for push_bar
get_proxy_lock = threading.Lock()
push_bar_lock = threading.Lock()

# Define a lock for logging
logging_lock = threading.Lock()
mutex = threading.Lock()

set_num = int(sys.argv[-1])
# set_num = 0
input(f"Set number: {set_num}")
logging.basicConfig(filename=f'./iaai_image_down_push_3.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
proxies = [x.strip() for x in open('proxies.txt').readlines()]
logging.info(f"Proxies Read from file. Number of proxies: {len(proxies)}")

def export_table_to_json_batch(table_name, source, batch_size=1000):
    try:
        connection = create_connection()
        print('Exporting data from db for iaai')
        logging.info('Exporting data from db for iaai')
        final_data = []
        with connection.cursor() as cursor:
            # Retrieve data from the table with parameterized query
            query = "SELECT * FROM {} WHERE `Source` = %s and `image_processing` = 'NOT PROCESSED';".format(table_name)
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
                print("iaai_ batch download: {}".format(len(lot_type)))
                logging.info("iaai_ batch download: {}".format(len(lot_type)))
            final_data = lot_type

        print("Data exported to JSON successfully.")
        logging.info("Data exported to JSON successfully.")
        return final_data
    except Exception as ex:
        print("Error exporting data: {}".format(ex))
def create_connection():
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

def return_formated_data(item,old_data):
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
        if data_dict["Image URLs"] == "NA":
            return None
        return data_dict

    return None
def reformat_data_for_interdb(item,vin_hidden):
    vin = vin_hidden
    document_type = item.get('type', '')
    sale_date = item.get('date', '')
    source = item.get('source','')
    lot = item.get('lot')
    details = ''
    for key, value in item.items():
        if key not in ['vin', 'type', 'date','source','lot', 'image_processing']:
            details += f"{key}: {value}\n"
    ITEM = (
        vin,
        document_type,
        datetime.strptime(sale_date, '%Y-%m-%d'),
        source,
        lot,
        details,
        "PROCESSED"
    )
    return ITEM
def execute_query_with_retry(query, values):
    attempts = 0
    retry_attempts = 10
    retry_delay = 5
    while attempts < retry_attempts:
        try:
            conn = create_connection()
            cursor = conn.cursor()

            cursor.executemany(query, values)
            conn.commit()

            cursor.close()
            conn.close()
            # print('Records successfully inserted or updated.')
            logging.info('Records successfully inserted or updated.')
            return  True# Exit the function if the query is executed successfully

        except mysql.connector.Error as ex:
            print(f'Error occurred: {ex}')
            logging.error(f'Error occurred: {ex}')
            attempts += 1
            logging.error(f'Retrying in {retry_delay} seconds...')
            time.sleep(retry_delay)

    logging.error('Exceeded maximum retry attempts. Cannot establish a connection.')
    return False
def get_random_proxy():
    with get_proxy_lock:
        return proxies[randint(0,len(proxies)-1)]
def get_random_user_agent():
    ua = UserAgent()
    return ua.random
def post_images(record):
    try:
        images = record["Image URLs"].split(' , ')
        data = {
            'vehicle': json.dumps(record),
        }
        for itr, img in enumerate(images): 
            retry_count = 0
            error_print = None
            while True:
                try:
                    proxy = get_random_proxy()
                    proxies = {
                        "http": proxy,
                        "https": proxy
                    }
                    headers = {
                        'User-Agent': get_random_user_agent(),
                        'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,'
                                'application/signed-exchange;v=b3',
                        'Accept-Encoding': 'gzip',
                        'Upgrade-Insecure-Requests': '1'
                    }
                    response = requests.get(
                        url=img.replace('&width=161&height=120', '&width=500&height=500'),
                        headers=headers,
                        proxies=proxies,
                        timeout=10
                    )
                    if response.status_code != 200:
                        continue
                    image_data = BytesIO(response.content)
                    data[f"data_files_r{itr}"] = (f"image_{record['lot']}_{itr + 1}.jpg", image_data, 'image/jpeg')
                    break
                except Exception as image_error:
                    retry_count +=1
                    error_print = image_error
                if retry_count>=5:
                    with logging_lock:
                        logging.error(f"Post Images Error => Record Error: {record['lot']}, {error_print}")
                
        response = requests.post('http://sdb-dev.vindb.org/api/input/multipart-form-data/singular', files=data)
        with logging_lock:
            logging.info(f"{response}: Image Posted > Lot Number: {record['lot']}. {response.text}")
        return True
    except Exception as ex:
        # print(f"Post Images Error: {ex}")
        with logging_lock:
            logging.error(f"Post Images Error: {ex}\nRecord Error: {json.dumps(record,indent=3)}")
    return False
def divide_list(input_list, num_parts=10):
    avg = len(input_list) // num_parts
    remainder = len(input_list) % num_parts
    result = []

    start = 0
    for i in range(num_parts):
        end = start + avg + (1 if i < remainder else 0)
        result.append(input_list[start:end])
        start = end

    return result


DATABASE_NAME='intermediate_db'
HOST='104.238.228.196'
username='root'
password='Crawler@2021'

interdb_records = export_table_to_json_batch("listing_records", "iaai")
formated_data = []
prev_vins = []
logging.info('iaai Formating Data')
df = pd.read_csv("{}".format(iaai_2_full_vin_csv),low_memory=False)
for itr in tqdm(range(len(df))):
    item = {
        "Lot#":str(df.iloc[itr]["Lot#"]).split('.')[0],
        "VIN":df.iloc[itr]["Vin#"]
    }
    temp_data = return_formated_data(item,interdb_records)
    if temp_data:
        prev_vins.append(interdb_records[item.get('Lot#')]["Vin"])
        formated_data.append(temp_data)

formated_data = divide_list(formated_data)[set_num]

print(len(formated_data))

bulk_values = []
logging.info(f"Preparing data for db push")
print(f"Preparing data for db push")
reformated_data = []
push_bar = tqdm(total=len(formated_data))
# Function to process records

def process_record(item, itr):
    if post_images(item):
        with push_bar_lock:
            reformated_data.append(reformat_data_for_interdb(item, prev_vins[itr]))
            push_bar.update()
# Use ThreadPoolExecutor to process records in multiple threads with max_workers set to 10
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    # Submit tasks for each record
    futures = [executor.submit(process_record, item, itr) for itr, item in enumerate(formated_data)]

    # Wait for all tasks to complete
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
push_bar.close()

batch_size = 100
print('Pushing Records To Database...')
logging.info('Pushing Records To Database...')
db_bar = tqdm(total=(len(reformated_data)/batch_size))
for i in range(0, len(reformated_data), batch_size):
    batch = reformated_data[i:i+batch_size]
    result = execute_query_with_retry(
        """
        INSERT INTO listing_records (Vin, Type, Date, Source, Lot, Details, image_processing)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Type = VALUES(Type),
            Date = VALUES(Date),
            Source = VALUES(Source),
            Lot = VALUES(Lot),
            Details = VALUES(Details),
            image_processing = VALUES(image_processing)
        """,
        batch
    )
    if result:
        logging.info(f'Inserted batch {i // batch_size + 1}/{len(formated_data) // batch_size}')
        db_bar.update(1)
