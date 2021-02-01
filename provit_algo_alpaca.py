import json
import logging
import threading
# from time import *
import time
import alpaca_trade_api as tradeapi

import pypyodbc
import configparser
import sys
import pytz
from datetime import datetime, timedelta
import math
import os

import websocket, json

socket = "wss://data.alpaca.markets/stream"


# init
logging.basicConfig(
    filename='errlog.log',
    level=logging.WARNING,
    format='%(asctime)s:%(levelname)s:%(message)s',
)

# loading configuration file
config = configparser.ConfigParser()
config.read('config.ini')
api_key = config["DEFAULT"]["APCA_API_KEY_ID"]
api_secret = config["DEFAULT"]["APCA_API_SECRET_KEY"]
live_key_id = config["DEFAULT"]["LIVE_APCA_API_KEY_ID"]
live_secret_id = config["DEFAULT"]["LIVE_APCA_API_SECRET_KEY"]
os.environ["APCA_API_KEY_ID"] = live_key_id
os.environ["APCA_API_SECRET_KEY"] = live_secret_id
base_url = 'https://paper-api.alpaca.markets'

manual_auto_flag = config["DEFAULT"]["MANUAL_AUTO_FLAG"]

api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')


g_connection = None
trade_msg = []
order_msg = []
past_trades = []

g_symbols = []
g_provit_price_symbols = {}
g_symbols_order_filled_status = []
g_symbols_order_cancelled_status = []
g_symbols_dropdown_step = {}


# connect to database
def connect_db(dbserver, dbname, dbuser, dbpass):
    # connection = pypyodbc.connect('Driver={SQL Server};'
    #                                 'Server=' + dbserver + ';'
    #                                 'Database=' + dbname + ';'
    #                                 'uid=' + dbuser + ';pwd=' + dbpass)
    # return connection

    conn = pypyodbc.connect(
        'Driver={SQL Server};' 'Server=DESKTOP-MEL3TAC;' 'Database=StockAlgo;')
    return conn

# disconnect from database
def disconnect_db():
    print("pre db disconnected")
    g_connection.close()
    print("db disconnected")

# get symbol data from tblSymbolWatchList
def get_symbols():

    SQLCommand = ("SELECT Symbol, OrderQuantity, TotalQuantity, OrderType, TargetSpread, StopSpread FROM tbl_stocks_pivot (nolock) "
                "WHERE OrderType = 'M' AND TradingFlag = 'Y'")
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchall()
    final_result = [list(i) for i in result]
    return final_result

# get the times until the market closes.
def time_to_market_close():
    clock = api.get_clock()
    closing = clock.next_close - clock.timestamp
    return round(closing.total_seconds() / 60)

# get the position of the specfic symbol
def get_position(symbol, positions):
    current_postion_id = ''
    for position in positions:
        if position.symbol == symbol:
            current_postion_id = position.asset_id
            break
    return current_postion_id
    

def send_order(symbol, order_qty, direction, current_price, tartget_profit_spread, stop_loss_spread):
    # if time_to_market_close() > 20:
    if direction == 'buy':
        sl = current_price - (float(tartget_profit_spread))
        tp = current_price + (float(tartget_profit_spread))
    elif direction == 'sell':
        sl = current_price + (float(tartget_profit_spread))
        tp = current_price - (float(tartget_profit_spread))

    print("start_order::::::::", direction, '++++++++++++++++++++')
    o = api.submit_order(
        symbol=symbol,
        qty=order_qty,
        side=direction,
        type='market',
        time_in_force='day',
        # order_class='bracket',
        stop_loss=dict(stop_price=str(sl)),
        take_profit=dict(limit_price=str(tp)),
    )
    my_order_variable = api.get_order_by_client_order_id(o.client_order_id)
    time.sleep(100)
    return my_order_variable

conn = tradeapi.stream2.StreamConn(api_key, api_secret, base_url)

@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    order_msg.append(account)


@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
    trade_msg.append(trade)
    # print(trade.order)
    if 'fill' in trade.event:
        past_trades.append(
            [
                trade.order['updated_at'],
                trade.order['symbol'],
                trade.order['side'],
                trade.order['filled_qty'],
                trade.order['filled_avg_price'],
            ]
        )
        with open('past_trades.csv', 'w') as f:
            json.dump(past_trades, f, indent=4)
        print(past_trades[-1])


def ws_start():
    conn.run(['account_updates', 'trade_updates'])

def wait_for_market_open():
    clock = api.get_clock()
    global g_provit_price_symbols
    if not clock.is_open:
        time_to_open = (clock.next_open - clock.timestamp).total_seconds()
        print(f'Need to wait {round(time_to_open)} s')
        time.sleep(round(time_to_open))
        g_provit_price_symbols = get_provit_price()

# get the profit value and loss value with symobls
def get_provit_price():
    provit_container = {}
    for g_symbol in g_symbols:
        symbol = g_symbol[0]
        previous_candlestick = api.get_barset(symbol, 'day', limit=1)
        provit_price = (previous_candlestick[symbol][0].h + previous_candlestick[symbol][0].l + previous_candlestick[symbol][0].c) / 3
        provit_container[symbol] = provit_price
    return provit_container

# check the stock to short available
def check_short_stock(symbols):
    temp_symbols = []
    for o in symbols:
        symbol = o[0]
        if (api.get_asset(symbol)).shortable == True:
            temp_symbols.append(o)
    return temp_symbols

# check order status
def check_order_status(symbol):
    check_orders = api.list_orders(status='open')
    for o in check_orders:
        if o.symbol == symbol:
            api.cancel_order(o.id)
            g_symbols_order_cancelled_status.append(o)
            break

    check_orders = api.list_orders(status='filled')
    for o in check_orders:
        if o.symbol == symbol:
            g_symbols_order_filled_status.append(o)
            return o

# log order info
def insert_order_info(order_info):
    print(order_info)
    SQLCommand = ("INSERT INTO tbl_orders_info_pivot ([OrderId], [Platform], [Symbol], [OrderInstruction], [OrderPrice], [OrderQty], [OrderStatus], "
                            "[OrderStartDate], [OrderUpdateDate]) "
                            "VALUES ('" + str(order_info.id) + "','Alpaca','" + order_info.symbol + "','" + order_info.side + "','" + order_info.filled_avg_price + "','"
                            + order_info.qty + "','" + order_info.status + "', GETDATE(), GETDATE())")
    print(SQLCommand)
    cursor = g_connection.cursor()
    cursor.execute(SQLCommand)
    g_connection.commit()

def main_thread_start():
    while True:
        for g_symbol in g_symbols:
            symbol = g_symbol[0]
            wait_for_market_open()
            polygon_data = tradeapi.REST(live_key_id, live_secret_id).polygon.snapshot(symbol)
            current_price = polygon_data.ticker['lastQuote']['P']
            tartget_profit_spread = g_symbol[4]
            stop_loss_spread = g_symbol[5]
            provit_price = g_provit_price_symbols[symbol]
            order_qty = g_symbol[1]
            print("Symbol:", symbol, "||", "ProvitPrice:", provit_price, "||", "Current Price:", current_price)
            if current_price >= provit_price:
                print("BUY", "tartget_profit_spread = ", tartget_profit_spread, "stop_loss_spread = ", stop_loss_spread)
                o = send_order(symbol, order_qty, 'buy', current_price, tartget_profit_spread, stop_loss_spread)
                insert_order_info(o)
                continue

            elif current_price <= provit_price:
                print("SELL", "tartget_profit_spread = ", tartget_profit_spread, "stop_loss_spread = ", stop_loss_spread)
                o = send_order(symbol, order_qty, 'sell', current_price, tartget_profit_spread, stop_loss_spread )
                insert_order_info(o)
                continue
    print("============Trade End==================")
    

if __name__ == "__main__":

    print("============Trade Start==================")

    # database connect
    g_connection = connect_db(config["DEFAULT"]["DB_SERVER"], config["DEFAULT"]
                              ["DB_NAME"], config["DEFAULT"]["DB_USER"], config["DEFAULT"]["DB_PASS"])
    
    while True:
        # check if market is open
        clock = api.get_clock()

        if clock.is_open:
            break
        else:
            time_to_open = (clock.next_open - clock.timestamp).total_seconds()
            print(f'Need to wait {round(time_to_open)} s')
            # time.sleep(time_to_open)
    
    
    # get the stock list and caculate provit price
    temp_symbols = get_symbols()
    # check if the stock is available to short
    g_symbols = check_short_stock(temp_symbols)
    g_provit_price_symbols = get_provit_price()

    # start WebSocket in a thread
    ws_thread = threading.Thread(target=ws_start, daemon=True)
    ws_thread.start()
    
    # start main thread
    main_thread = threading.Thread(target=main_thread_start)
    main_thread.start()

 