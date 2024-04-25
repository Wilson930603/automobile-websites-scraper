import os
import datetime
import threading
import time
# Change the current working directory to "root/auto-mobile"
os.chdir("/root/automobile-project")

date_today = datetime.datetime.now().strftime("%Y_%m_%d")
def run_spider(spider_name, output_file):
    os.system('/usr/local/bin/scrapy crawl ' + spider_name + ' -o ' + output_file)

copart_thread = threading.Thread(target=run_spider, args=('copart_fullVIN', './datafolder/copart_FULL_VIN_'+date_today+'.csv'))
ca_copart_thread = threading.Thread(target=run_spider, args=('ca_copart_fullVIN', './datafolder/ca_copart_FULL_VIN_'+date_today+'.csv'))
iaai_thread = threading.ThreadError(target=run_spider,args=('iaai_2','./datafolder/iaai_2_FULL_VIN_'+date_today+'.csv'))
ca_copart_thread.start()
time.sleep(200)
copart_thread.start()
time.sleep(200)
iaai_thread.start()

ca_copart_thread.join()
copart_thread.join()
iaai_thread.join()