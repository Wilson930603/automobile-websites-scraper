# Automobile Project
The project extracts auction data from copart and iaai websites and pushes them to db.

## Part 1
### Database Integration
The script includes a section in the closed method in both spiders that pushes the extracted data to a database. You need to replace the code inside this section with your own database integration logic. The provided code demonstrates bulk insertion or update of the extracted data into a database table named "listing_records" with appropriate columns.

Please ensure you have the necessary database connectivity and modify the code accordingly to match your database structure and credentials in the pipeline.py

### 1. IAAI Spider
This script is a web scraping spider built using Scrapy framework. There are two scripts for IAAI. One extracts from iaai.com domain and the other extracts from iaai.ca domain. It is specifically designed to extract data from the IAAI (Insurance Auto Auctions) website. The spider visits various pages on the website to collect information about vehicle listings and stores the extracted data in a database.

### Installation
1. Clone the project to your local machine.
2. Install the required libraries by running the following command:
```bash
pip install -r requirements.txt
```
### Prerequisites
- Python >= 3.6.8

### Usage
- Make sure you have a working internet connection.

- Open a terminal or command prompt and navigate to the directory where the script is located.

- Run the script using the following command:
- For iaai.com domain
```bash
scrapy crawl iaai -o ./datafolder/iaai_data.json -a page=1
```
- At max 10 pages can be added, but there should be one page number given in the terminal
- For iaai.ca domain
```bash
scrapy crawl ca_iaai -o ./datafolder/ca_iaai_data.json
```
### Functionality
1. The spider starts by visiting the branch locations page to extract the URLs of upcoming auctions.
2. For each auction, it visits the listing page to extract the URLs of individual vehicle listings.
3. For each vehicle listing, it visits the information page to extract various details about the vehicle.
4. The extracted data is then stored in a database.
5. The spider supports pagination and can navigate through multiple pages of vehicle listings.
6. The spider uses rotating proxies to make requests and avoids getting blocked by the website.
7. Progress of the spider's execution of uploading data to db is displayed using the tqdm library.

### Logging
- The script logs the iaai.com data and its output to a log file located in the "iaai_logs" directory. The log file's name includes the spider's name and the current date.
- The script logs the iaai.ca data and its output to a log file located in the "ca_iaai_logs" directory. The log file's name includes the spider's name and the current date.

### 2. Copart Spider
This script is a web crawler built using the Scrapy framework. It crawls the Copart website (https://www.copart.com/), (https://www.copart.ca/)and  to extract information about salvage car auctions. The script retrieves data from various pages on the website, including search results and lot details. There are two scirpts copart.py extracts data from copart.com domain and the copart_ca.py extracts the data from copart.ca domain.

### Installation
1. Clone or download the script to your local machine.
2. Install the required libraries by running the following command:

```bash
pip install -r requirements.txt
```

### Usage
- Make sure you have a working internet connection.

- Open a terminal or command prompt and navigate to the directory where the script is located.

- Run the script using the following command:

- For extracting data from copart.com domain use: 
```bash
scrapy crawl copart -o ./datafolder/copart_data.json
```

- For extracting data from copart.ca domain use:
```bash
scrapy crawl ca_copart -o ./datafolder/ca_copart_data.json
```
### Logging
- The script logs the copart.com data and its output to a log file located in the "copart_logs" directory. The log file's name includes the spider's name and the current date.
- The script logs the copart.ca data and its output to a log file located in the "ca_copart_logs" directory. The log file's name includes the spider's name and the current date.


## Part 2
Part 2 consists of extracting FULL VINs along with lot information from copart and iaai. The extracted lot number is checked if it is available in the intermediate_db. If it is available then the record is extracted from intermediate_db incomplete vin is replaced with the FULL VIN, and then the data is pushed to the api.

### 1. IAAI Spider
IAAI uses the combination of scrapy and selenium along with 2 captcha api service to bypass the captcha and extract the full vins.

### Installation
1. Clone the project to your local machine.
2. Install the required libraries by running the following command:

```bash
pip install -r requirements.txt
```

### Usage
- Make sure you have a working internet connection.

- Open a terminal or command prompt and navigate to the directory where the script is located.

- Run the script using the following command:

- For extracting data from copart.com domain use: 
```bash
scrapy crawl iaai_2 -o ./datafolder/iaai_2.csv
```
### Logging
- The script logs the iaai.com data and its output to a log file located in the "iaai_2" directory. The log file's name includes the spider's name and the current date.

### 2. Copart Spider
Copart also uses both selenium and scrapy to extract the full vin from copart.com and copart.ca. It first logs in and extracts the cookies which is passed in scrapy request.

### Installation
1. Clone the project to your local machine.
2. Install the required libraries by running the following command:

```bash
pip install -r requirements.txt
```

### Usage
- Make sure you have a working internet connection.

- Open a terminal or command prompt and navigate to the directory where the script is located.

- Run the script using the following command:

- For extracting data from copart.com domain use: 
```bash
scrapy crawl copart_fullVIN -o ./datafolder/copart_fullVIN.csv
```
```bash
scrapy crawl ca_copart_fullVIN -o ./datafolder/ca_copart_fullVIN.csv
```
### Logging
- The script logs the copart.com data and its output to a log file located in the "copart_fullVIN" directory. The log file's name includes the spider's name and the current date.
- The script logs the copart.ca data and its output to a log file located in the "ca_copart_fullVIN" directory. The log file's name includes the spider's name and the current date.

## wrapper
There are wrapper scirpts available to run all scipts of part 1 and to run all scripts of part 2.
- To run them, use the following commands:
```bash
python wraper.py
``` 
```bash
python wrapper_full_vin.py
```

## wrapper copart
There is wrapper script that is named wraper_copart that is to run the copart spider, and then extract the images using headless selenium browsers. In the end the intermediate db is updated. To run the wraper_copart script, use the following command
```bash
python wraper_copart.py
```

## iaai sdb pushing
`iaai_image_down_push.py` reads the vin numbers and lot and matches it with the extacted data (whose images are not pushed to the sdb) and then downloads them in a number of 10 threads and then pushes the records one by one to sdb server. To run this script there must be a a iaai_2_full_vin.csv file present in the directory. To run the script first update the variable `iaai_2_full_vin_csv` in the script. The run the script using this command:
```bash
python iaai_image_down_push.py
```
The logs will be created will it will show progress of the completed file on terminal aswell.
## Cronjob
To set up cronjob for to run part 1 and part 2 do the following steps:
- Open the linux terminal, and enter the following commands
```bash
export VISUAL=nano
``
```bash
crontab -e
```
- A text box will open, in which jobs can be added to run at specific time frame. Enter the desired time, along with the path of python, and the path of the scirpt. For example:
```bash
PATH=/usr/local/bin:/usr/bin:/bin
0 1 * * * /usr/bin/python3 /root/automobile-project/wraper.py > /root/automobile-project/cron.log 2>&1
```
- In this example the wraper.py script which runs all the part 1 spiders. Will execute every day at 1 AM, and the logs will be saved to cron.log
- after adding the relavant time and paths; save the file.
- Now the cronjob will be active. You can check by using the following command:
```bash
crontab -l
```