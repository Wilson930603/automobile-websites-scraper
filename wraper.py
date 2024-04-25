import os
import datetime
import threading
# Change the current working directory to "root/auto-mobile"
os.chdir("/root/automobile-project")

def run_spider(spider_name, output_file):
    os.system('/usr/local/bin/scrapy crawl ' + spider_name + ' -o ' + output_file)

def run_spider_iaai(spider_name, output_file,page):
    os.system('/usr/local/bin/scrapy crawl ' + spider_name + ' -o ' + output_file + ' -a page='+str(page))


date_today = datetime.datetime.now().strftime("%Y_%m_%d")

# copart_thread = threading.Thread(target=run_spider, args=('copart', './datafolder/copart_upload_'+date_today+'.json'))
ca_copart_thread = threading.Thread(target=run_spider, args=('ca_copart', './datafolder/ca_copart_upload_'+date_today+'.json'))
# ca_iaai_thread = threading.Thread(target=run_spider, args=('ca_iaai', './datafolder/ca_iaai_upload_'+date_today+'.json'))


# copart_thread.start()
ca_copart_thread.start()
# ca_iaai_thread.start()

iaai = []

for i in range(1,11):
    iaai.append(threading.Thread(target=run_spider_iaai,args=('iaai', './datafolder/iaai_upload_'+date_today+'_page'+str(i)+'.json',i)))
for job in iaai:
    job.start()

# copart_thread.join()
ca_copart_thread.join()
# ca_iaai_thread.join()
for job in iaai:
    job.join()

