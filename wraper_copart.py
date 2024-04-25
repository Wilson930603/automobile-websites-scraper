import pandas as pd
import os
from datetime import datetime
import threading
import json
from tqdm import tqdm
import mysql.connector 
from mysql.connector import errorcode
import logging
import time

# Change the current working directory to "root/auto-mobile"
# os.chdir("/root/automobile-project")
file_name = "copart_images_step"
step_range = 6
current_date = datetime.now().strftime("%Y-%m-%d")
saved_file = f"./datafolder/copart_{current_date}.json"
if not os.path.exists(f'./copart_logs'):
    os.makedirs(f'./copart_logs')
    
# os.system(f'/usr/local/bin/scrapy crawl copart -o {saved_file}')
os.system(f'scrapy crawl copart -o {saved_file}')
logging.basicConfig(filename=f'./copart_logs/copart_wrapper_{current_date}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')




def run_spider(spider_name, output_file,step):
    # os.system('/usr/local/bin/scrapy crawl ' + spider_name + ' -o ' + output_file)
    # os.system('/usr/local/bin/scrapy crawl ' + spider_name + ' -o ' + output_file + ' -a ' + step)
    os.system('scrapy crawl ' + spider_name + ' -o ' + output_file + ' -a ' + step)




def combine_csv():
    files = []
    for itr in range(step_range):
        # Define the file names for the two CSV files
        file_one = f"./datafolder/{file_name}_{itr}.csv"
        # Read data from the first CSV file
        data_one = pd.read_csv(file_one)
        files.append(data_one)


        # Combine the data from both files
    combined_data = pd.concat(files, ignore_index=True)

    # Define the file name for the new combined CSV file
    combined_file = f"./datafolder/{file_name}.csv"

    # Write the combined data to a new CSV file
    combined_data.to_csv(combined_file, index=False)

    for itr in range(step_range):
        # Define the file names for the two CSV files
        file_one = f"./datafolder/{file_name}_{itr}.csv"
        os.remove(file_one)



def read_json_file(json_file_path):

    # Read the JSON file
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
    
    new_json = {}

    for item in tqdm(data):
        new_json[str(item["Lot"])] = item
    

    return new_json

def return_formated_data_interdb(item):
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
        "Date":datetime.strptime(sale_date, '%Y-%m-%d'),
        "Source":source,
        "Lot":lot,
        "Details":details}
    return ITEM
def return_formated_data(item,old_data):
    # Split the string into lines
    
    if old_data.get(item.get('Lot#')):
        lines = old_data[item.get('Lot#')]["Details"].split('\n')
        # Extract keys and values from the lines and create a dictionary
        data_dict = {line.split(': ')[0]: line.split(': ')[1] for line in lines if ': ' in line}
        data_dict["vin"] =  old_data[item.get('Lot#')]["vin"]
        data_dict["type"] = old_data[item.get('Lot#')]["Type"]
        data_dict["date"] = old_data[item.get('Lot#')]["Date"]
        data_dict["source"] = old_data[item.get('Lot#')]["Source"]
        data_dict["lot"] = old_data[item.get('Lot#')]["Lot"]
        data_dict["Image URLs"] = item["Image URLs"]
        new_data = return_formated_data_interdb(data_dict)
        return new_data

    return None
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
def execute_query_with_retry(query, values):
    attempts = 0
    retry_attempts = 3
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
            return  # Exit the function if the query is executed successfully

        except mysql.connector.Error as ex:
            print(f'Error occurred: {ex}')
            logging.error(f'Error occurred: {ex}')
            attempts += 1
            print(f'Retrying in {retry_delay} seconds...')
            logging.error(f'Retrying in {retry_delay} seconds...')
            time.sleep(retry_delay)

    print('Exceeded maximum retry attempts. Cannot establish a connection.')
    logging.error('Exceeded maximum retry attempts. Cannot establish a connection.')




threads = []
for step in range(step_range):
    threads.append(threading.Thread(target=run_spider,args=("copart_images",f"./datafolder/{file_name}_{step}.csv",f"steps={step}")))

for thread in threads:
    thread.start()
    time.sleep(150)
for thread in threads:
    thread.join()
combine_csv()
tables = ['listing_records']

interdb_records = read_json_file(saved_file)

DATABASE_NAME='intermediate_db'
HOST='104.238.228.196'
username='root'
password='Crawler@2021'
print(mysql.connector.__version__)

formated_data = []
if len(interdb_records)>0:
    print('Copart Formating Data')
    logging.info('Copart Formating Data')
    df = pd.read_csv("{}".format(f"./datafolder/{file_name}.csv"),low_memory=False)
    for itr in tqdm(range(len(df))):
        item = {
            "Lot#":str(df.iloc[itr]["Lot#"]).split('.')[0],
            "Image URLs":df.iloc[itr]["Image URLs"],    
        }
        temp_data = return_formated_data(item,interdb_records)
        if temp_data:
            formated_data.append(temp_data)
logging.info(f"Total values: {len(formated_data)}")
bulk_values = []
logging.info(f"Preparing data for db push")
print(f"Preparing data for db push")
for item in tqdm(formated_data):
    vin = item['vin']
    document_type = item['Type']
    sale_date = item['Date']
    source = item['Source']
    lot = item["Lot"]
    details = item['Details']
    bulk_values.append((vin, document_type, sale_date, source, lot, details))

batch_size = 100
print('Pushing Records To Database...')
logging.info('Pushing Records To Database...')
for i in tqdm(range(0, len(bulk_values), batch_size)):
    batch = bulk_values[i:i+batch_size]
    execute_query_with_retry(
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
    logging.info(f'Inserted batch {i // batch_size + 1}/{len(formated_data) // batch_size}')

os.remove(f"./datafolder/copart_images_{current_date}.csv")
os.remove(f"./datafolder/copart_{current_date}.json")
os.remove(f"./datafolder/{file_name}.csv")
# thread_copart_img_push.join()
# os.remove(f"{saved_file_full_vin}")
logging.info(f"Copart finished")

