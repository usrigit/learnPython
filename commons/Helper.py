"""
This file contains all utility functions which are common for all modules
"""

import functools
import traceback
import requests
import re, json, datetime
from bs4 import BeautifulSoup
from collections import OrderedDict


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
                    # print("DATA script = ", data_script)
                    data = data_script.split("data")[2]
                    data = data[3:]
                    for ele in data.split(","):
                        x = ele.split(":")[0]
                        y = ele.split(":")[1]
                        if '"x"' in x:
                            epoch_time = int(trim_special_chr(y))
                            eps_year = datetime.datetime.fromtimestamp(epoch_time / 1000).strftime('%Y-%m-%d %H:%M:%S')[
                                       :4]
                        if '"y"' in x:
                            ele_arr[eps_year] = float(trim_special_chr(y))
            return ele_arr
    except Exception as err:
        print("ERROR in get_eps_data", str(err))

    return None


def extract_float(input_str):
    if input_str is None or input_str == '' or input_str == '-':
        return 0.0
    out_number = ''
    for ele in input_str:
        if ele.isdigit() or ele == ".":
            out_number += ele
    if out_number:
        return float(out_number)
    else:
        return 0.0


def trim_special_chr(input_str):
    input_str = re.sub(r"[()\"#/%-@;:<>{}[\]`+=~|!?,]", "", input_str)
    return input_str


def alpnum_to_num(str):
    try:
        if len(str) > 0:
            return ''.join(filter(lambda x: x.isdigit(), str))

    except Exception as e:
        pass
    return 0


def parse_url(url):
    try:
        # This will fetch the content of the URL
        res = requests.get(url, timeout=5)
        # This will raise status if any error occurs
        if not res.raise_for_status():
            return BeautifulSoup(res.text, "html.parser")
        else:
            return None
    except Exception as err:
        print("Error while parsing {} url is {} ".format(url, err))
        raise err


def merge_dict_with_sub_dict(dict1, dict2):
    final_dict = {}
    for k in set().union(dict1, dict2):
        val = OrderedDict()
        sub_d1 = dict1.get(k)
        sub_d2 = dict2.get(k)

        if sub_d1:
            val.update(sub_d1)
        if sub_d2:
            val.update(sub_d2)
        final_dict.update({k: val})
    return final_dict


def write_list_to_json_file(filename, data_list):
    # open output file for writing
    with open(filename, 'w') as file:
        json.dump(data_list, file)
    file.close()

def read_list_from_json_file(filename):
    output = []
    # open output file for reading
    with open(filename, 'r') as file:
        output = json.load(file)
    file.close()
    return output

