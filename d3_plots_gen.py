import os
from bs4 import BeautifulSoup
import glob
import logging
import geo_stat
import staking

def gen_all_plots_js(logger=None):
    if not logger:
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
        logger = logging.getLogger("html_generation")

    def extract_js(html_path):
        with open(html_path, "r") as f:
            text = f.read()
            bs = BeautifulSoup(text, "html.parser")
            scripts = bs.find_all('script')
            mpld_script = scripts[-1]

            with open(html_path.split(".")[0]+".js", "w") as f_out:
                f_out.write(mpld_script.text)

    geo_stat.plot_geostat_update("html_out/fig_geostat.html", timespan=60, save_as_html=True)
    geo_stat.plot_city_ranking("html_out/fig_cityranking.html", save_as_html=True)
    geo_stat.plot_country_stat("html_out/fig_onlinestats.html", save_as_html=True)
    geo_stat.plot_interactive_stargate_hosts("html_out/fig_stargate_hosts.html", logger)
    staking.plot_payouts("html_out/fig_payouts.html", save_as_html=True)
    staking.plot_staked("html_out/fig_staked.html", save_as_html=True)
    
    for path in glob.glob("html_out/fig_*.html"):
        extract_js(path)

    for path in glob.glob("html_out/fig_*.html"):
        os.remove(path)


if __name__ == '__main__':
	gen_all_plots_js()