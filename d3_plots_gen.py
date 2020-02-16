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

    if not os.path.exists("html_out"):
         os.makedirs("html_out")
    geo_stat.plot_city_ranking("html_out/fig_cityranking.html", save_as_html=True)

    for sizeclass in ["xl", "lg", "md", "sm"]:
        geo_stat.plot_interactive_stargate_hosts("html_out/fig_stargate_hosts_{}.html".format(sizeclass), logger, d3_scale=sizeclass)
        geo_stat.plot_geostat_update("html_out/fig_geostat_{}.html".format(sizeclass), timespan=60, save_as_html=True, d3_scale=sizeclass)
        geo_stat.plot_country_stat("html_out/fig_onlinestats_{}.html".format(sizeclass), save_as_html=True, d3_scale=sizeclass)
        staking.plot_payouts("html_out/fig_payouts_{}.html".format(sizeclass), save_as_html=True, d3_scale=sizeclass)
        staking.plot_staked("html_out/fig_staked_{}.html".format(sizeclass), save_as_html=True, d3_scale=sizeclass)
    
    for path in glob.glob("html_out/fig_*.html"):
        extract_js(path)

    for path in glob.glob("html_out/fig_*.html"):
        os.remove(path)


if __name__ == '__main__':
	gen_all_plots_js()
