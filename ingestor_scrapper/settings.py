"""
Scrapy settings for ingestor_scrapper project.

This file contains the basic configuration for Scrapy.
Additional settings can be added here for pipelines, middlewares, etc.
"""

BOT_NAME = "ingestor_scrapper"

SPIDER_MODULES = ["ingestor_scrapper.interface.spiders"]
NEWSPIDER_MODULE = "ingestor_scrapper.interface.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# User-Agent for the spider
USER_AGENT = "ingestor_scrapper (+http://www.yourdomain.com)"

# Configure delays for requests (optional, can be adjusted later)
# DOWNLOAD_DELAY = 0.5
# RANDOMIZE_DOWNLOAD_DELAY = 0.5

# Enable or disable spider middlewares
# SPIDER_MIDDLEWARES = {
#     "ingestor_scrapper.middlewares.SpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# DOWNLOADER_MIDDLEWARES = {
#     "ingestor_scrapper.middlewares.DownloaderMiddleware": 543,
# }

# Enable or disable extensions
# EXTENSIONS = {
#     "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# TODO: Activate pipelines when storage/processing logic is needed
# ITEM_PIPELINES = {
#     "ingestor_scrapper.pipelines.ExamplePipeline": 300,
# }

# Auto-throttle settings (optional)
# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 1
# AUTOTHROTTLE_MAX_DELAY = 60
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# AUTOTHROTTLE_DEBUG = False

# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = "INFO"
