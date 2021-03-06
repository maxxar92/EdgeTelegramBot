import pandas as pd
import sqlite3
from sqlite3 import Error
from flask import Flask, request, jsonify, abort
import threading
from telegram import ParseMode
import scrape_new_hosts as host_scraper
import traceback
import logging
import json

CLIENT_DB = "registeredClients.db"
app = Flask(__name__)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


@app.route('/api/update/<token>', methods=['POST'])
def update(token):
    try:
        content = request.get_json(silent=True)
        msg = content["update_message"]
        user_updater.send_update_message(token, msg)
    except Exception as e:
        logger.exception(e)
        abort(500, e)
    
    return "ok"

@app.route('/api/host/<device_uuid>', methods=['GET'])
def host_status(device_uuid):
    try:
        hosts = host_scraper.read_hosts_from_db()
        retrieved_host = hosts.loc[hosts.device_id == device_uuid]
        if retrieved_host.empty:
            logger.exception("UUID was not found.")
            return abort(500, "UUID was not found.")
        
        status = retrieved_host.status.iloc[0]
        if status == "Offline" or status == "Status unknown":
            return "offline"

        if status == "Just now" or status == "Less than a minute ago":
            return "online"

        nominator, timescale, _ = status.split(" ")
        multiplicators = {"minute": 1, "hour": 60, "day": 60 * 24, "week": 60*24*7, "month": 60*24*30}
        if timescale.endswith("s"):
            timescale = timescale[:-1] #remove s from hours, weeks, etc.

        minutes_offline = multiplicators[timescale] * int(nominator)
        return "minutes_offline={}".format(minutes_offline)

    except Exception as e:
        logger.exception(e)
        return abort(500, traceback.format_exc())


@app.route('/api/allhosts', methods=['GET'])
def host_list():
    try:
        hosts = host_scraper.read_hosts_from_db()
        hosts = hosts[["device_id", "host_name", "stargate", "location", "status"]]
        hosts["countrycode"] = ["" if loc == "-" else loc.split(",")[-1].strip() for loc in hosts.location]
        hosts.set_index('device_id', inplace=True)
        return jsonify(json.loads(hosts.to_json(orient='index')))
    except Exception as e:
        logger.exception(e)
        return abort(500, traceback.format_exc())

@app.route('/api/gateways', methods=['GET'])
def gateway_list():
    try:
        gateways = host_scraper.read_gateways_from_db()
        gateways = gateways[["device_id", "stargate", "location", "arch", "status"]]
        gateways["countrycode"] = ["" if loc == "-" else loc.split(",")[-1].strip() for loc in gateways.location]
        gateways.set_index('device_id', inplace=True)
        return jsonify(json.loads(gateways.to_json(orient='index')))
    except Exception as e:
        logger.exception(e)
        return abort(500, traceback.format_exc())


@app.route('/api/stargates', methods=['GET'])
def stargate_list():
    try:
        stargates = host_scraper.read_stargates_from_db()
        stargates = stargates[["device_id", "stargate_name", "location", "arch", "status"]]
        stargates["countrycode"] = ["" if loc == "-" else loc.split(",")[1].strip() for loc in stargates.location]
        stargates.set_index('device_id', inplace=True)
        return jsonify(json.loads(stargates.to_json(orient='index')))
    except Exception as e:
        logger.exception(e)
        return abort(500, traceback.format_exc())

def flaskthread():
    app.run(host='0.0.0.0', port=5000, threaded=True)

class UserUpdates(object):
    def register(self, telegram_id, device_token, host_uuid):
        conn, cur = self.get_db_conn()
        with conn:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS registered (
                    device_token INTEGER PRIMARY KEY,
                    telegram_id TEXT NOT NULL
                );
            """)
            # The first command will insert the record. If the record exists, it will ignore the error caused by the conflict with an existing primary key.
            cur.execute("INSERT OR IGNORE INTO registered (device_token, telegram_id) VALUES ({}, '{}');".format(device_token, telegram_id))
            cur.execute("UPDATE registered SET telegram_id='{}' WHERE device_token={};".format(telegram_id, device_token))

    def unregister(self, telegram_id, device_token):
        conn, cur = self.get_db_conn()
        cur.execute("DELETE FROM registered WHERE device_token={} AND telegram_id='{}';".format(device_token, telegram_id))

    def get_telegram_id(self, device_token):
        conn, cur = self.get_db_conn()
        result = pd.read_sql('select * from registered where device_token={}'.format(device_token), conn)
        if not result.empty:
            return result.telegram_id[0]
        else:
            return None

    def get_db_conn(self):
        conn = sqlite3.connect(CLIENT_DB)
        cur = conn.cursor()
        return conn, cur

    def parse_messgae(msg):
        pass

    def send_update_message(self, device_token, msg):
        chat_id = self.get_telegram_id(device_token)
        if chat_id is not None:
            self.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.MARKDOWN)


    def start_api_server(self, bot):
        self.bot = bot
        threading.Thread(target=flaskthread).start()

user_updater = UserUpdates()