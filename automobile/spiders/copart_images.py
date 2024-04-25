import scrapy
import json
import sys
from selenium import webdriver
from time import sleep
from selenium.webdriver.common.by import By as by
import pandas as pd
from twocaptcha import TwoCaptcha
from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime
import time
import os
class CrawlerSpider(scrapy.Spider):
    name = 'copart_images'
    api_key='d368d93ac4830dc4a52b5351a7b92626'
    current_date = datetime.now().strftime("%Y-%m-%d")
    try:
        key = int(sys.argv[-1].split('=')[-1])
    except:
        key = None
    if not os.path.exists(f'./{name}_logs'):
        os.makedirs(f'./{name}_logs')
    custom_settings = {
        'LOG_FILE': f'./{name}_logs/{name}_{current_date}_step_{key}.log',
        # 'CLOSESPIDER_ITEMCOUNT': 50,
    }
    img_file_name = f"./datafolder/{name}_{current_date}.csv"
    
    def recaptcha_v2(self,sitekey, url,api_key):
        try:
            solver = TwoCaptcha(api_key)
            # result = solver.solve_captcha(site_key=sitekey, page_url=url)
            result = solver.recaptcha(sitekey=sitekey, url=url)
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
            # if answer:
            if answer["code"]:
                print("Token:", answer)
                driver.execute_script("""document.getElementById("g-recaptcha-response").innerHTML = arguments[0]""", answer["code"])
                # driver.execute_script("""document.getElementById("g-recaptcha-response").innerHTML = arguments[0]""", answer)
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
                driver.execute_script(scriptTxt,answer["code"],post_url)
                # driver.execute_script(scriptTxt,answer,post_url)
                print("Solved recaptcha_v2 successfully.")
                self.logger.info("Solved recaptcha_v2 successfully.")
            else:
                self.logger.error("Failed to solve captcha")
            driver.switch_to.default_content()

    def getCookies(self):
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        driver = webdriver.Firefox(options=options)
        # driver = webdriver.Firefox(executable_path = GeckoDriverManager().install(), options=options)
        driver.get('https://www.copart.com/locations/')
        if '_Incapsula_Resource?SWUDNSAI=' in driver.page_source:
            driver.save_screenshot('./captcha.png')
            print('1) Found Captcha')
            self.logger.info('1) Found Captcha')
            self.by_pass(driver)
            sleep(20)

        cookies = driver.get_cookies()
        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
        cookies = cookies_dict.copy()
        driver.close()
        return cookies


    
        

    newcookies = None
    def start_requests(self):
        cookies = self.getCookies()
        steps = int(sys.argv[-1].split('=')[-1])
        print(steps)        
        headers = {'Host': 'www.copart.com','Connection': 'keep-alive','Cache-Control': 'max-age=0','Upgrade-Insecure-Requests': '1','User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36','DNT': '1','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8','Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,tr;q=0.7'}
        print(self.img_file_name)
        urls = pd.read_csv(f"{self.img_file_name}")['img_url'].tolist()
        for num in range(steps,len(urls),2):
            url = urls[num]
            yield scrapy.Request(url,callback=self.getImages,dont_filter=True,headers=headers,cookies=cookies,meta={'steps':steps,'headers':headers,'again':False})

    def getImages(self,response):

        meta = response.meta
        headers = meta.get('headers')
        steps = meta.get('steps')
        again = meta.get('again')
        try:
            data = json.loads(response.text)
        except:
            if self.newcookies == None or again:
                self.newcookies = self.getCookies()
            yield scrapy.Request(response.url,callback=self.getImages,dont_filter=True,headers=headers,cookies=self.newcookies,meta={'headers':headers,'steps':steps,'again':True})
            return

        items = {}
        lotdata = data["data"]["lotDetails"]
        if lotdata == None:
            lotdata = {}
        color = lotdata.get('clr')
        lotno = lotdata.get('ln',response.url.split('/')[-2])

        try:
            images = " , ".join([img.get('url') for img in data["data"]["imagesList"]["HIGH_RESOLUTION_IMAGE"]])
        except:
            images = " , ".join([img.get('url') for img in data["data"]["imagesList"]["FULL_IMAGE"]])

        items['Lot#'] = lotno
        items["Color"] = color
        items["Image URLs"] = images
        print(f"Step: {steps} - Success: {lotno}")
        yield items