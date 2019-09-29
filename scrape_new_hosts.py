import os
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from sqlite3 import Error

# don't split tables when printing
pd.set_option('expand_frame_repr', False)

HOST_DB = "hosts.db"

def poll_new_hosts(logger):
    if os.path.isfile(HOST_DB):
        hosts_df = scrape_hosts_table()
        new_hosts = get_new_hosts(hosts_df)
        if not new_hosts.empty:
            # sanity check
            if new_hosts.shape[0] <= 5:
                write_hosts_to_db(new_hosts)
                return new_hosts
            else:
                logger.warning("More than 5 new hosts detected. Probably the bot was down for a longer time. Recreating host table, not sending notifications.")
                clear_db()
                fill_new_db(hosts_df)
                logger.info("Created new DB with host table: ")
                logger.info(hosts_df)
    else:
        hosts_df = fill_new_db()
        logger.info("Created new DB with host table: ")
        logger.info(hosts_df)
    
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
        row = [tr.text for tr in td]
        host_list.append(row)
    return pd.DataFrame(host_list, columns=["device_id", "host_name", "stargate", "location", "arch", "status"])


def get_new_hosts(scraped_hosts):
    conn = sqlite3.connect(HOST_DB)
    read_hosts = pd.read_sql('select * from hosts', conn)
    return scraped_hosts[~scraped_hosts.device_id.isin(read_hosts.device_id)]

def write_hosts_to_db(hosts):
    conn = sqlite3.connect(HOST_DB)
    # write to db
    hosts.to_sql(name='hosts', con=conn, if_exists="append")

def fill_new_db(hosts_df=None):
    if hosts_df is None:
        hosts_df = scrape_hosts_table()
    conn = sqlite3.connect(HOST_DB)
    # write to db
    hosts_df.to_sql(name='hosts', con=conn, if_exists="replace")
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