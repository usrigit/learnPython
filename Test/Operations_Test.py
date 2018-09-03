import commons.Helper as h

dict1 = {'key1': [1, 2, 3], 'key2': [11, 21, 31]}
dict2 = {'key1': [4, 5], 'key2': [41, 51]}

print({k: dict1.get(k, []) + dict2.get(k, []) for k in set().union(dict1, dict2)})

dict1 = {'key1': {'a': 1, 'b': 2}}
dict2 = {'key1': {'x': 3, 'y': 4}, 'key2': {'p': 10, 'q': 11, 'r': 12}}

final_dict = {}
for k in set().union(dict1, dict2):
    val = {}
    sub_d1 = dict1.get(k)
    sub_d2 = dict2.get(k)

    if sub_d1:
        val.update(sub_d1)

    if sub_d2:
        val.update(sub_d2)
    final_dict.update({k: val})
print("Final dict = ", final_dict)

print("Valu = ", ''.join(filter(lambda x: x.isdigit(), "VOLUME  6,438")))

ratio_con = {
    'Basic EPS (Rs.)': "EPS",
    'Book Value [Excl. Reval Reserve]/Share (Rs.)': "BOOK_VAL",
    'Dividend/Share (Rs.)': "DIVIDEND",
    'Return on Equity / Networth (%)': "RET_ON_EQ",
    'Price To Book Value (X)': "PRC_TO_BOOK",
    'Price To Sales (X)': "PRC_TO_SALE",
    'Current Ratio (X)': "CURR_RATIO",
    'Total Debt/Equity (X)': "DEBT_EQUITY"
}

ratio_elements = {
    'YEAR': [],
    'EPS': [],
    'BOOL_VAL': [],
    'DIVIDEND': [],
    'RET_ON_EQ': [],
    'PRC_TO_BOOK': [],
    'PRC_TO_SALE': [],
    'CURR_RATIO': [],
    'DEBT_EQUITY': []
}
stock_ratio = {}
url = "https://www.moneycontrol.com/financials/yesbank/ratiosVI/YB#YB"
bs = h.parse_url(url)
if bs:
    std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
    print(std_data.text.split("|")[1])
    data = [
        [
            [td.string.strip() for td in tr.find_all('td') if td.string]
            for tr in table.find_all('tr')[2:]
        ]
        for table in bs.find_all("table", {"class": "table4"})[2:]
    ]
    ele_list = data[0]
    stock_ratio['YEAR'] = data[0][0]
    i = 2
    while i < len(ele_list) - 4:
        arr = ele_list[i]
        if len(arr) > 5:
            key = ratio_con.get(arr[0])
            val = arr[1:]
            if key: stock_ratio[key] = val
        i += 1
    print("STK RATIO = ", stock_ratio)