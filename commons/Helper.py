"""
This file contains all utility functions which are common for all modules
"""

import functools
import traceback
import requests
from bs4 import BeautifulSoup


def catch_exceptions(job_func):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        try:
            job_func(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())

    return wrapper()


def upd_dic_with_sub_dic(p_key, c_key, c_val, p_dic):
    tmp_dic = {}
    if p_key in p_dic:
        tmp_dic = p_dic.get(p_key)
    tmp_dic[c_key] = c_val
    p_dic[p_key] = tmp_dic


def upd_dic_with_sub_list(p_key, val, p_dic):
    if p_key in p_dic:
        temp_val = p_dic.get(p_key)
        temp_val.append(val)
    else:
        temp_val = [val]
        p_dic[p_key] = temp_val


def parse_url(url):
    # This will fetch the content of the URL
    res = requests.get(url)
    # This will raise status if any error occurs
    if not res.raise_for_status():
        return BeautifulSoup(res.text, "html.parser")
    else:
        return None
