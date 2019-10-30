import unittest
import os
import mock
import pandas as pd
import scrape_new_hosts as scraper
import logging
from datetime import datetime
import geo_stat

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.ERROR)

logger = logging.getLogger(__name__)



current_mock_scrape_data = {}
def mock_scrape_hosts():
    return pd.DataFrame(current_mock_scrape_data)

def append_entry(device_id, host_name, location, stargate, arch, status):
    current_mock_scrape_data["device_id"].append(device_id)
    current_mock_scrape_data["host_name"].append(host_name)
    current_mock_scrape_data["location"].append(location)
    current_mock_scrape_data["arch"].append(arch)
    current_mock_scrape_data["status"].append(status)
    current_mock_scrape_data["stargate"].append(stargate)

class TestCreateDB(unittest.TestCase):

    def setUp(self):
        scraper.HOST_DB = "test_hosts.db"
        current_mock_scrape_data["device_id"] = ["id-1", "id-2"]
        current_mock_scrape_data["host_name"] = ["test-host1", "test-host2"]
        current_mock_scrape_data["location"] = ["Miami", "-"]
        current_mock_scrape_data["arch"] = ["arm", "amd64"]
        current_mock_scrape_data["stargate"] = ["mia", "ams"]
        current_mock_scrape_data["status"] = ["Online", "Offline"]

    def tearDown(self):
        current_mock_scrape_data.clear()
        scraper.clear_db()

    @mock.patch('scrape_new_hosts.scrape_hosts_table', side_effect=mock_scrape_hosts)
    def test_create_db(self, scrape_func):
        scraper.fill_new_db()
        con, cur = scraper.get_db_conn()
        columns = [i[1] for i in cur.execute('PRAGMA table_info(hosts)')]

        assert len(columns) == 9
        assert "online_notification" in columns and "first_online_timestamp" in columns

        db_hosts = scraper.read_hosts_from_db()
        for timestamp in db_hosts.first_online_timestamp.values:
            assert timestamp is None
    
    @mock.patch('scrape_new_hosts.scrape_hosts_table', side_effect=mock_scrape_hosts)
    def test_poll(self, scrape_func):
        scraper.fill_new_db()
        new_hosts = scraper.poll_new_hosts(logger)
        assert new_hosts is None

        append_entry("test-id3", "test-host3", "Berlin, DE", "fra", "amd64", "Offline")
        new_hosts = scraper.poll_new_hosts(logger)
        assert new_hosts is None
    
        db_hosts = scraper.read_hosts_from_db()
        assert "test-id3" in  db_hosts.device_id.values

        # last appended host came online
        current_mock_scrape_data["status"][-1] = "Online"
        # will trigger timestamp and pending update
        new_hosts = scraper.poll_new_hosts(logger)
        assert len(new_hosts) == 1
        assert "test-id3" in new_hosts.device_id.values

        db_hosts = scraper.read_hosts_from_db()
        new_row = db_hosts.loc[db_hosts.device_id == "test-id3"]
        assert new_row.status.values[0] == "Online"
        assert new_row.first_online_timestamp.values[0] != None
        assert new_row.online_notification.values[0] == "done"

        timestamp = new_row.first_online_timestamp.values[0]
        assert datetime.fromtimestamp(float(timestamp)).year >= 2019


class TestPlottingRealData(unittest.TestCase):
    def setUp(self):
        scraper.HOST_DB = "testdata/hosts_snapshot_30_10.db"


    def testAddedPlot(self):
        db_hosts = scraper.read_hosts_from_db()

        timespans = list(range(1,10)) + [11,15,17,20,25,30,41]
        for timespan in timespans:
            print("testing plot with timespan: {}".format(timespan))
            testout_filename =  "testout/out_added_nodes_{}.png".format(timespan)
            fig = geo_stat.plot_geostat_update(testout_filename, timespan)
            fig.savefig(testout_filename, dpi=200,bbox_inches="tight")





if __name__ == '__main__':
    unittest.main()