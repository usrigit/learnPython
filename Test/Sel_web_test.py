from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import os

url = "https://www.moneycontrol.com/ipo/ipo-snapshot/listed-ipos.html"

# create a new Chrome session
driver = webdriver.Chrome(executable_path="D:/Projects/LearnPython/Chrome/chromedriver")
driver.get(url)
print(driver.find_element_by_xpath('//*[@onclick="2"]'))
