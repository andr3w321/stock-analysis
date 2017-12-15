import good_morning as gm
import requests
from bs4 import BeautifulSoup
from helper import sp500_2017
from yahoo_historical import Fetcher as yf
import datetime

DATA_FOLDER = "./data/"

def download_sp500():
    """ Print to output, then copy and paste to helper.py """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#Recent_changes_to_the_list_of_S&P_500_Components"
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "lxml")
    sp_500 = {}
    tables = soup.find_all("table")
    # current tickers
    trs = tables[0].find_all("tr")
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) > 0:
            sp_500[tds[0].find("a").text] = {"name": tds[1].find("a").text, "current": True}
    # old tickers
    trs = tables[1].find_all("tr")
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) > 3 and tds[3].text != "Ticker":
            if len(tds) == 4:
                ticker_td = tds[2]
                name_td = tds[3]
            else:
                ticker_td = tds[3]
                name_td = tds[4]
            if ticker_td.text != "":
                sp_500[ticker_td.text] = {"name": name_td.find("a").text, "current": False}
    return sp_500
        
def download_key_ratios(tickers):
    kr = gm.KeyRatiosDownloader()
    for ticker in tickers:
        print(ticker)
        try:
            kr_frames = kr.download(ticker)
            for kr_frame in kr_frames:
                kr_frame.to_csv("{}{}-{}.csv".format(DATA_FOLDER, ticker, kr_frame.index.name.replace("/","&")))
        except Exception as e:
            print("failed", e)

def download_financials(tickers):
    kr = gm.FinancialsDownloader()
    for ticker in tickers:
        print(ticker)
        try:
            kr_fins = kr.download(ticker)
            for key in kr_fins:
                if key == "currency":
                    if kr_fins[key] != "USD":
                        raise ValueError("ERROR non USD currency")
                    else:
                        pass
                elif key in ["period_range","fiscal_year_end"]:
                    pass
                else:
                    kr_fins[key].to_csv("{}{}-{}.csv".format(DATA_FOLDER, ticker, key))
        except Exception as e:
            print("failed", e)

    print(ticker, end='')
    try:
        kr.download(ticker, conn)
        fd.download(ticker, conn)
        time.sleep(1)
        print(' ... success')
    except Exception as e:
        print('failed', e)

def download_price_history(tickers):
    now = datetime.datetime.now()
    for ticker in tickers:
        print(ticker)
        # edge case, morning star uses diff symbols than yahoo finance occasionally
        if ticker == "BF.B":
            mod_ticker = "BF-B"
        elif ticker == "BRK.B":
            mod_ticker = "BRK-B"
        else:
            mod_ticker = ticker
        try:
            data = yf(mod_ticker, [1995,1,1], [now.year, now.month, now.day], "1d")
            data.getHistorical().to_csv("{}{}-price-history.csv".format(DATA_FOLDER, ticker))
        except Exception as e:
            print("failed", e)

#download_key_ratios(sp500_2017)
#download_financials(sp500_2017)
download_price_history(sp500_2017)
#print(download_sp500())
