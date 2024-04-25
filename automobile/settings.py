
BOT_NAME = "automobile"

SPIDER_MODULES = ["automobile.spiders"]
NEWSPIDER_MODULE = "automobile.spiders"

ROBOTSTXT_OBEY = False
HTTPERROR_ALLOW_ALL=True
TELNETCONSOLE_ENABLED = False

ROBOTSTXT_OBEY = False


# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    "automobile.pipelines.AutomobilePipeline": 300,
# }

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
