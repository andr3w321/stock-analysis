from helper import sp500_2017
import pandas as pd
import os

DATA_FOLDER = "./data/"

def get_rois(end_year, years_held=1):
    rois = {}
    for ticker in sp500_2017:
        filename = "{}{}-price-history.csv".format(DATA_FOLDER, ticker)
        if os.path.isfile(filename) is False:
            print("Skipping {} due to no price data".format(ticker))
        else:
            df = pd.read_csv(filename)
            if "Date" not in df.columns:
                print("Can't find Date column for {}".format(ticker))
            else:
                if end_year == 2017:
                    end_date = '2017-12-14'
                else:
                    end_date = "{}-12-31".format(end_year)
                price_df = df[(df['Date'] >= '{}-12-31'.format(end_year - 1 - years_held)) & (df['Date'] <= end_date)]
                if len(price_df) > 1:
                    start_price = price_df.iloc[0]["Adj Close"]
                    end_price = price_df.iloc[-1]["Adj Close"]
                    rois[ticker] = (end_price - start_price) / start_price * 100.0
    output = []
    for k,v in sorted(rois.items(), key=lambda x: x[1], reverse=True):
        output.append(["{:.2f}%".format(v),k,sp500_2017[k]["name"]])
    return pd.DataFrame(output, columns=["{}-{}".format(end_year - years_held, end_year + 1),"ticker","company"])

#for years_held in [20]:
for years_held in [1,2,3,4,5,10,15,20]:
    print("Doing {} years_held".format(years_held))
    df = pd.DataFrame()
    for year in range(1995 + years_held, 2018):
        df = pd.concat([df, get_rois(year,years_held)], axis=1)
        
    df.to_csv("./output/{}-year-snp-returns.csv".format(years_held))
