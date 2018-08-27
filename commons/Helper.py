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


def get_eps_data(url):
    try:
        bs = parse_url(url)
        if bs:
            all_scripts = bs.find_all('script')
            ele_arr = {}
            for number, script in enumerate(all_scripts):
                if 'netProfitEPSChart =' in script.text:
                    start = script.text.find("data")
                    end = script.text.find("netProfitEPSChart =")
                    data_script = script.text[start:end]
                    list_arr = data_script.split("data")[2].split(",")
                    year = 2013
                    i = 0
                    for ele in list_arr:
                        if '"y":' in ele:
                            ele_arr[year + i] = extract_nbr(ele)
                            i += 1
    except Exception as err:
        print(err)

    return ele_arr


def extract_nbr(input_str):
    if input_str is None or input_str == '':
        return 0
    out_number = ''
    for ele in input_str:
        if ele.isdigit() or ele == ".":
            out_number += ele
    return float(out_number.strip("."))


def parse_url(url):
    # This will fetch the content of the URL
    res = requests.get(url, timeout=5)
    # This will raise status if any error occurs
    if not res.raise_for_status():
        return BeautifulSoup(res.text, "html.parser")
    else:
        return None
