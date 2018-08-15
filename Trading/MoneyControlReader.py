import bs4, requests

shares = {}
shr_category = {}
money_cntrl_url = 'https://www.moneycontrol.com/india/stockpricequote'
res = requests.get(money_cntrl_url)
res.raise_for_status()
bs = bs4.BeautifulSoup(res.text, "html.parser")
table = bs.find("table", {"class": "pcq_tbl MT10"})

for row in table.findAll("tr"):
    for link in row.findAll("a"):
        shares[link.get("title")] = link.get('href')


def upd_shr_dic(key, tkey, tval, dest_dic):
    tmp_dic = {}
    if key in dest_dic:
        tmp_dic = dest_dic.get(key)
        tmp_dic[tkey] = tval
        # dest_dic[key] = tmp_dic
    else:
        tmp_dic[tkey] = tval
        dest_dic[key] = tmp_dic


for title, link in shares.items():
    elements = link.split("/")
    if len(elements) > 5:
        key = elements[5]
        upd_shr_dic(key, title, link, shr_category)

print("Shr category = ", shr_category.keys())

for category, cat_dic in shr_category:
    for title, link in cat_dic:
        pass
