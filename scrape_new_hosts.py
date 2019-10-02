import os
import requests
from requests import get
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from sqlite3 import Error

# don't split tables when printing
pd.set_option('expand_frame_repr', False)


# don't retry connection
s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=0))


HOST_DB = "hosts.db"

def poll_new_hosts(logger):
    if os.path.isfile(HOST_DB):

        try:
            hosts_df = scrape_hosts_table()
        except Exception as e:
            logger.exception("[poll_new_hosts::scrape_hosts_table]", e)
        new_hosts = get_new_hosts(hosts_df)

        if not new_hosts.empty:
            # sanity check
            if new_hosts.shape[0] <= 5:
                logger.info("Found new hosts: \n"+str(new_hosts))
                online_hosts = new_hosts.loc[new_hosts.status == "Online"]
                write_hosts_to_db(online_hosts)
                
                offline_hosts = new_hosts.loc[new_hosts.status != "Online"]
                write_hosts_to_db(offline_hosts, online_notification="pending")
                logger.info("Marked following hosts as pending. Notification will be sent once it comes online.\n"+str(offline_hosts))
                return online_hosts
            else:
                logger.warning("More than 5 new hosts detected. Probably the bot was down for a longer time. Recreating host table, not sending notifications.")
                clear_db()
                fill_new_db(hosts_df)
                logger.info("Created new DB with host table: ")
                logger.info(hosts_df)
        else:
            update_hosts_db(hosts_df) # update all information in db (except pending status)
            pending_online = get_first_online_hosts(hosts_df)
            if not pending_online.empty:
                logger.info("Following pending hosts have come online:\n"+str(pending_online))
                set_pending_to_done(pending_online) # online notification only once
                return pending_online

    else:
        hosts_df = fill_new_db()
        logger.info("Created new DB with host table: \n"+  str(hosts_df))
    
    return None

def scrape_hosts_table():
    url = 'https://explorer.edge.network/'
    response = get(url)
    html_soup = BeautifulSoup(response.text, 'html.parser')

    hosts_header_div = html_soup.find("div", id="hosts")
    host_table = hosts_header_div.find("table")
    #find all rows and exclude header
    host_rows = host_table.find_all("tr")[1:] 
    host_list = []
    for tr in host_rows:
        td = tr.find_all('td')
        row = [tr.text.strip() for tr in td]
        host_list.append(row)
    return pd.DataFrame(host_list, columns=["device_id", "host_name", "stargate", "location", "arch", "status"])

def update_hosts_db(scraped_hosts):
    conn = sqlite3.connect(HOST_DB)
    cur = conn.cursor()
    scraped_hosts.to_sql('updatetable', conn, if_exists='replace')
    cur.execute("UPDATE hosts " + \
              "SET stargate = (SELECT stargate FROM updatetable WHERE hosts.device_id = updatetable.device_id ), " + \
              "location = (SELECT location FROM updatetable WHERE hosts.device_id = updatetable.device_id ), " + \
              "status = (SELECT status FROM updatetable WHERE hosts.device_id = updatetable.device_id ) " + \
              "WHERE device_id IN (SELECT device_id FROM updatetable);")
    conn.commit()


def get_new_hosts(scraped_hosts):
    conn = sqlite3.connect(HOST_DB)
    read_hosts = pd.read_sql('select * from hosts', conn)
    return scraped_hosts[~scraped_hosts.device_id.isin(read_hosts.device_id)]

def get_first_online_hosts(scraped_hosts):
    conn = sqlite3.connect(HOST_DB)
    read_hosts = pd.read_sql('select * from hosts', conn)#
    pending_notification_hosts = read_hosts.loc[read_hosts.online_notification == "pending"]
    pending_scraped = scraped_hosts[scraped_hosts.device_id.isin(pending_notification_hosts.device_id)]
    return pending_scraped.loc[pending_scraped.status == "Online"]

def set_pending_to_done(update_hosts_df):
    conn = sqlite3.connect(HOST_DB)
    cur = conn.cursor()
    update_hosts_df.to_sql('updatetable', conn, if_exists='replace')
    cur.execute("UPDATE hosts " + \
              "SET online_notification = 'done' " + \
              "WHERE device_id IN (SELECT device_id FROM updatetable);")
    conn.commit()

def read_hosts_from_db():
    conn = sqlite3.connect(HOST_DB)
    read_hosts = pd.read_sql('select * from hosts', conn)
    return read_hosts

def write_hosts_to_db(hosts, online_notification="done"):
    conn = sqlite3.connect(HOST_DB)
    notif_status_hosts = hosts.assign(online_notification=[online_notification]*hosts.shape[0])
    # write to db
    notif_status_hosts.to_sql(name='hosts', con=conn, if_exists="append")

def fill_new_db(hosts_df=None):
    if hosts_df is None:
        hosts_df = scrape_hosts_table()
    conn = sqlite3.connect(HOST_DB)
    notif_status_hosts = hosts_df.assign(online_notification=["done"]*hosts_df.shape[0])
    # write to db
    notif_status_hosts.to_sql(name='hosts', con=conn, if_exists="replace")
    return hosts_df

def clear_db():
    conn = sqlite3.connect(HOST_DB)
    sql = 'DELETE FROM hosts'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def print_new_hosts():
    hosts_df = scrape_hosts_table()
    new_hosts = get_new_hosts(hosts_df)
    if not new_hosts.empty:
        print("new hosts:")
        print(new_hosts)
    else:
        print("no new hosts")


if __name__ == '__main__':
    if os.path.isfile(HOST_DB):
        print_new_hosts()
    else:
        fill_new_db()
