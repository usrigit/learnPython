import requests
from bs4 import BeautifulSoup


# url = 'http://www.moneycontrol.com/india/stockpricequote/diversified/3mindia/MI42'
# print(url.split("/")[5])

results = {}


def mny_ctr_shr_frm_url(cmp_name, cmp_url):

    dict_comp = {}
    dict_comp["NAME"] = cmp_name
    res = requests.get(cmp_url)
    res.raise_for_status()
    bs = BeautifulSoup(res.text, "html.parser")
    std_data = bs.find('div', {'id': 'mktdet_1'})
    for each_div in std_data.findAll('div', attrs={'class': 'PA7 brdb'}):
        sub_div = each_div.descendants
        __tag_name, __tag_value = None, None
        for cd in sub_div:
            if cd.name == 'div' and cd.get('class', '') == ['FL', 'gL_10', 'UC']:
                __tag_name = cd.text
            if cd.name == 'div' and cd.get('class', '') == ['FR', 'gD_12']:
                __tag_value = cd.text
            if __tag_name and __tag_value:
                dict_comp[__tag_name] = __tag_value
                __tag_name, __tag_value = None, None
    results[cmp_name] = dict_comp


if __name__ == "__main__":
    title = 'Cyient'
    link = 'http://www.moneycontrol.com/india/stockpricequote/computerssoftware/cyient/IE07'
    comp_details = mny_ctr_shr_frm_url(title, link)
    print(comp_details)
