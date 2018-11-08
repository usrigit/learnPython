import pickle
from bs4 import BeautifulSoup
from commons import Helper as h


# url = 'http://www.moneycontrol.com/india/stockpricequote/diversified/3mindia/MI42'
# print(url.split("/")[5])

results = {}


def get_bonus_mc():
    mc_file = 'D:/Projects/work/Trading/sql/mc_stocks.csv'
    mc_bonus_file = 'D:/Projects/work/Trading/sql/mc_bonus_stocks.csv'
    with open(mc_file) as f:
        content = f.readlines()
    buffer = []
    for line in content:
        data = line.split("/")
        code = data[-1]
        cmp_name = data[-2]
        link = 'https://www.moneycontrol.com/company-facts/' + \
               cmp_name + "/bonus/" + code + "#" + code + "\n"
        print(link)
        buffer.append(link)
    with open(mc_bonus_file, 'wb') as fp:
        pickle.dump(buffer, fp)

    # with open(mc_bonus_file, 'rb') as fp:
    #     links = pickle.load(fp)
    #     for link in links:
    #         cmp_details = get_bonus_split_details(link)
    #         print("DIV details = ", cmp_details)



def get_bonus_split_details(cmp_url):
    bs = h.parse_url(cmp_url)
    cmp_name = None
    title_data = bs.find('div', {'id': 'nChrtPrc'}).find('h1', {'class': 'b_42 PT20'})
    if title_data:
        cmp_name = title_data.text.strip()
    std_data = bs.find('div', {'class': 'PB10'}).find('div', {'class': 'FL gry10'})
    if std_data:
        header_parts = std_data.text.split("|")
        nse_code, isin, sector = None, None, None
        for part in header_parts:
            name, val = part.split(":")[0].strip(), part.split(":")[1].strip()
            if name == 'ISIN': isin = val
            if name == 'NSE': nse_code = val
            if name == 'SECTOR': sector = val
        if not nse_code: nse_code = isin
        print("nse code = {}, isin = {}, sector = {}".format(nse_code, isin, sector))
    data = [
        [
            [td.string.strip() for td in tr.find_all('td') if td.string]
            for tr in table.find_all('tr')[1:]
        ]
        for table in bs.find_all("table", {"class": "tbldivid"})
    ]
    print(data)
    bonus_count = len(data[0])
    print("bonus count = ", bonus_count)
    last_bonus_dt = data[0][0][0]
    if "-" not in last_bonus_dt:
        bonus_count = 0
        last_bonus_dt = '01-01-1900'

    split_url = cmp_url.replace("bonus", "splits")
    bs = h.parse_url(split_url)
    data = [
        [
            [td.string.strip() for td in tr.find_all('td') if td.string]
            for tr in table.find_all('tr')[1:]
        ]
        for table in bs.find_all("table", {"class": "tbldivid"})
    ]
    print(data)
    split_times = len(data[0])
    last_split_dt = data[0][0][0]
    if "-" not in last_split_dt:
        split_times = 0
        last_split_dt = '01-01-1900'

    div_url = cmp_url.replace("bonus", "dividends")
    bs = h.parse_url(div_url)
    data = [
        [
            [td.string.strip() for td in tr.find_all('td') if td.string]
            for tr in table.find_all('tr')[1:]
        ]
        for table in bs.find_all("table", {"class": "tbldivid"})
    ]
    print("data = ", data)
    div_cnt = len(data[0])
    print("div_cnt= ", div_cnt)
    last_div_dt = data[0][0][0]

    if split_times == 0 and bonus_count == 0:
        return None
    else:
        cmp_details = {}
        cmp_details['STK_CODE'] = nse_code
        cmp_details['SECTOR'] = sector
        cmp_details['BONUS_COUNT'] = bonus_count
        cmp_details['LST_BONUS_DT'] = last_bonus_dt
        cmp_details['SPLIT_COUNT'] = split_times
        cmp_details['LST_SPLIT_DT'] = last_split_dt
        cmp_details['DIV_COUNT'] = div_cnt
        cmp_details['LST_DIV_DT'] = last_div_dt
        return cmp_details

if __name__ == "__main__":
    title = 'Cyient'
    link = 'https://www.moneycontrol.com/company-facts/sirhindenterprises/dividends/SE13#SE13'
    # link = 'https://www.moneycontrol.com/company-facts/utiniftyexchangetradedfund/bonus/UTI19#UTI19'
    # get_bonus_mc()
    cmp_details = get_bonus_split_details(link)
    print("DIV details = ", cmp_details)
