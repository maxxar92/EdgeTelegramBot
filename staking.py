import os
from etherscan.accounts import Account
import matplotlib.pyplot as plt
import logging
import pandas as pd
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import sqlite3
import numpy as np
from collections import OrderedDict

def get_stakes():
    with open('config.json') as config_file:
        data = json.load(config_file)

    if "etherscan_api_token" not in data.keys() or data['etherscan_api_token'] == "":
        raise Exception("etherscan_api_token must be specified in config.json")
          

    key = data['etherscan_api_token']
    stake_address = '0x370f4d7625c10499bbb9353f3e91f83a0b750bec'

    api = Account(address=stake_address, api_key=key)
    transactions = api.get_transaction_page(page=1, offset=10000, sort='des', erc20=True)
    total_in = 0
    total_out = 0
    stargates=0
    for t in transactions:
        if t["tokenName"] == "DADI" and t["to"] == stake_address:
            value = int(t["value"]) / 1e18
            total_in += value
            if value == 5e5:
                stargates += 1
        elif t["from"] == stake_address and t["to"] != "0xef45b79def79a2cd2d0c77f84fddaab8d0b8be35":
            total_out = value
            if value == 5e5:
                stargates -= 1
    total_staked = total_in - total_out 
    stargates_staked = stargates * 5e5
    hosts_staked = total_staked - stargates_staked

    return total_staked, hosts_staked, stargates_staked

def plot_staked(out_filename):
    total_supply = 100e6
    total_staked, host_stake, stargate_stake = get_stakes()

    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = 'Non-Staked', 'Stargates', 'Hosts'
    sizes = [total_supply-total_staked, stargate_stake, host_stake]
    explode = (0, 0.1, 0.1) 

    fig, ax = plt.subplots()
    ax.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=False, startangle=0)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    fig.savefig(out_filename,dpi=200,bbox_inches="tight")
    plt.close(fig)


def load_from_cmc_json():
    data = json.load(open("testdata/historical_prices_dadi.json", "r"),  object_pairs_hook=OrderedDict)["data"]
    columns = ["date", "BTC", "ETH", "USD"]
    rows = []
    for datestr, values in data.items():
        date = pd.to_datetime(datestr)
        btc = values["BTC"][0]
        eth = values["ETH"][0]
        usd = values["USD"][0]
        rows.append([date, btc, eth, usd])
    df = pd.DataFrame(data=rows, columns=columns)
    df.set_index('date', inplace=True)

    return df

    # conn = sqlite3.connect(PRICE_DB)
    # cur = conn.cursor()
    # with conn:
    #     df.to_sql(name='prices', con=conn, if_exists="replace")
    # conn.close()

def get_prices():
    with open('config.json') as config_file:
        data = json.load(config_file)

    if "coinmarketcap_api_token" not in data.keys() or data['coinmarketcap_api_token'] == "":
        raise Exception("coinmarketcap_api_token must be specified in config.json")

    key = data['coinmarketcap_api_token']
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {
      'symbol': "DADI",
      "convert": "BTC"
    }
    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': key,
    }

    prices = {"BTC": "", "ETH": "", "USD": ""}
    for convert_symbol in prices.keys():
        parameters["convert"] = convert_symbol

        session = Session()
        session.headers.update(headers)

        try:
          response = session.get(url, params=parameters)
          data = json.loads(response.text)
          quote = data["data"]["DADI"]["quote"][convert_symbol]
          prices[convert_symbol] = quote["price"]

        except (ConnectionError, Timeout, TooManyRedirects) as e:
          print(e)
          return None

    return prices

def add_payout(dadi):
    with open('testdata/payouts.json', 'r') as payouts_file:
        data = json.load(payouts_file)
    data["payouts"].append(dadi)
    with open("testdata/payouts.json", "w") as outfile:
        json.dump(data, outfile, indent=4)


def check_new_prices(logger):
    df = load_from_cmc_json()
    lastrow = df.tail(1)
    if pd.to_datetime(lastrow.index[0]).year < pd.to_datetime('today').tz_localize("UTC").year or pd.to_datetime(lastrow.index[0]).dayofyear < pd.to_datetime('today').tz_localize("UTC").dayofyear:
        new_price = get_prices()
        if new_price is not None:
            new_price_data = {  "BTC": [new_price["BTC"]], "ETH": [new_price["ETH"]], "USD": [new_price["USD"]] }
            logger.info("polled new price: " + str(new_price_data))
            data = json.load(open("testdata/historical_prices_dadi.json", "r"),  object_pairs_hook=OrderedDict)
            data["data"][str(pd.to_datetime('today').tz_localize("UTC").round('1s'))] = new_price_data
            with open("testdata/historical_prices_dadi.json", "w") as outfile:
                json.dump(data, outfile, indent=4)

def plot_payouts(out_filename):
    with open('testdata/payouts.json') as payouts_file:
        data = json.load(payouts_file)

    payouts = data["payouts"]
    startmonth = pd.Timestamp(year=2019, month=4, day=1, hour=12)

    df = load_from_cmc_json()
    usd_payouts = []
    cur_month = startmonth
    for payout in payouts:
        prices = df[(df.index.year == cur_month.year) & (df.index.month == cur_month.month)]
        prices= prices["USD"]
        # usd_payout / dadiprice1 * 1/days + usd_payout / dadiprice2 * 1/days = totaldadi
        usd_payouts.append(payout * len(prices) / np.reciprocal(prices.to_numpy()).sum())
        cur_month = cur_month + pd.DateOffset(months=1)

    r = pd.date_range(start=startmonth.date(), periods=len(payouts), freq='MS')    
    fig, ax_linegraph = plt.subplots(1, 1)
    ax_linegraph.set_title("Monthly payouts")

    color = "blue"
    ax_linegraph.set_ylabel("USD", color=color)
    usd_series = pd.Series(data=usd_payouts, index=r)
    usd_series.plot(ax=ax_linegraph,linestyle='--', marker='o',color=color,markersize=10)
    ax_linegraph.tick_params(axis='y', labelcolor=color)

    color = "green"
    dadi_ax = ax_linegraph.twinx()
    dadi_ax.set_ylabel('EDGE', color=color)  # we already handled the x-label with ax1
    dadi_series = pd.Series(data=payouts, index=r)
    dadi_series.plot(ax=dadi_ax, linestyle='--', marker='o',color=color,markersize=10)
    dadi_ax.tick_params(axis='y', labelcolor=color)

    fig.savefig(out_filename, dpi=200,bbox_inches="tight")
    plt.close(fig)
