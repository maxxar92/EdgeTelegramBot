import time
import logging
import pandas as pd
import geopandas
import geopy
from geopy.geocoders import Nominatim, MapBox
from geopy.extra.rate_limiter import RateLimiter
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import scrape_new_hosts as host_scraper

plt.rcParams["font.size"] = 14

# because the lat/long positions on the explorer are not known for individual hosts, we geocode (lookup) the lat/long position from the city/country
def geocode_locations(locations, logger, use_mapbox=True):
    if use_mapbox:
        # mapbox key stolen from node explorer :D
        geolocator = MapBox(api_key="pk.eyJ1IjoiZGFkaWNvIiwiYSI6IlVaOVU3QmMifQ.dDoTL04V_WtMo2iQkfvZBg")
    else:
        geolocator = Nominatim(user_agent="edgenodelookup")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1,swallow_exceptions=False)
    def lookup(location):
        city, countrycode = [s.strip() for s in location.split(",")]
        logger.info(u"geocode processing {} {}".format(city, countrycode))
        try:
            if use_mapbox:
                result = geocode(unicode(city), country=countrycode)
            else:
                result = geocode({"city":city}, country_codes=countrycode)
        except Exception as e:
            logger.error(u"geocode processing failed with {}".format(e))
            return None
        if result is None:
            logger.warning(u"geocode could not find {} {}. None is used.".format(city, countrycode))
        return result

    decoded_location = locations.apply(lookup)
    decoded_location_non_nil = decoded_location[pd.notna(decoded_location)] #remove nil
    # seperate location object into parts
    latitudes = decoded_location_non_nil.apply(lambda loc: loc.latitude)
    longitudes = decoded_location_non_nil.apply(lambda loc: loc.longitude)
    addresses = decoded_location_non_nil.apply(lambda loc: loc.address)
    loc_df = pd.DataFrame({"latitude": latitudes, 'longitude': longitudes, 
        'retrieved_address': addresses,  
        'explorer_location': locations.loc[pd.notna(decoded_location)]})

    return loc_df

def fill_location_lookup_db(logger):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df = hosts_df.loc[hosts_df.location != "-"]
    locations = hosts_df['location'].drop_duplicates()
    loc_df = geocode_locations(locations, logger)
    conn, cur = host_scraper.get_db_conn()
    with conn:
        loc_df.to_sql(name='map_locations', con=conn, if_exists="replace")

def locations_table_exists():
    conn, cur = host_scraper.get_db_conn()
    with conn:
        # check if table named map_locations exists
        cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='map_locations' ''')
        return cur.fetchone()[0]==1 # 1 == exists
    
def cache_new_locations(new_hosts_df, logger):
    if not locations_table_exists():
        fill_location_lookup_db()

    conn, cur = host_scraper.get_db_conn()
    new_hosts_df = new_hosts_df.loc[new_hosts_df.location != "-"]
    locations = new_hosts_df['location'].drop_duplicates()
    cached_locations = pd.read_sql('select * from map_locations', conn)

    new_locations = locations[~locations.isin(cached_locations.explorer_location)]
    if new_locations.empty:
        return
    loc_df = geocode_locations(new_locations, logger)
    with conn:
        loc_df.to_sql(name='map_locations', con=conn, if_exists="append")


def retrieve_cached_locations():
    conn, cur = host_scraper.get_db_conn()
    cached_locations = pd.read_sql('select * from map_locations', conn)
    return cached_locations


def plot_geostat_update(timespan):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df_timed = hosts_df[pd.notna(hosts_df["first_online_timestamp"])].copy() 
    hosts_df_timed["datetime"] = pd.to_datetime(hosts_df_timed.first_online_timestamp * 1e9)
    start_day = pd.Timestamp.today() - pd.Timedelta(days=timespan)
    hosts_df_timed = hosts_df_timed.loc[hosts_df_timed.datetime >= start_day]

    fig, (ax_map,ax_linegraph) = plt.subplots(2, 1,figsize=(14, 14), gridspec_kw={'height_ratios': [6, 1],"hspace":-0.5})
    ax_map.axis('off')
    ax_map.set_title("New host locations (last {} days)".format(timespan))
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world  = world[(world.pop_est>0) & (world.name!="Antarctica")]
    world.plot(ax=ax_map, color="lightblue",edgecolor="#6ec4cc")
    ax_map.set_aspect('equal')
    ax_map.set_ylim(-60, 88)
    ax_map.set_xlim(-185, 185)

    loc_df = retrieve_cached_locations()
    hosts_df_timed_known_loc = hosts_df_timed.loc[(hosts_df_timed.location!="-") & 
                                                  hosts_df_timed.location.isin(loc_df.explorer_location) ]
    # equal column names needed for pd.merge
    df_renamed = hosts_df_timed_known_loc.rename(columns={"location": "explorer_location"}) 
    merged_df = pd.merge(df_renamed, loc_df, how='inner', on=['explorer_location'])

    geo_df_data = pd.DataFrame(
        {'City': [loc.split(",")[0] for loc in merged_df.explorer_location],
         'Country': [loc.split(",")[-1] for loc in merged_df.retrieved_address],
         'Latitude': merged_df.latitude,
         'Longitude': merged_df.longitude
    })

    gdf = geopandas.GeoDataFrame(
        geo_df_data, geometry=geopandas.points_from_xy(geo_df_data.Longitude, geo_df_data.Latitude))
    gdf.plot(ax=ax_map, color='red',marker=7,markersize=50,edgecolor="black")

    hosts_unknown_locs = hosts_df_timed[~hosts_df_timed.host_name.isin(hosts_df_timed_known_loc.host_name)]
    if not hosts_unknown_locs.empty:
        ax_map.text(-45,-50,"Hosts with unknown locations: {}".format(hosts_unknown_locs.shape[0]))


    ax_linegraph.yaxis.set_major_locator(MaxNLocator(integer=True))
    counts = hosts_df_timed.groupby([hosts_df_timed.datetime.dt.date])["datetime"].count()
    r = pd.date_range(end=pd.Timestamp.today().date(),periods=timespan)
    counts = counts.reindex(r).fillna(0.0) #fill dates without new hosts
    ax_linegraph.set_title("New hosts online - per day")
    ax_linegraph.set_ylabel("Hosts")
    counts.plot(ax=ax_linegraph,linestyle='--', marker='o',color="red")
    out_filename = "added_nodes.png"
    fig.savefig(out_filename,dpi=200,bbox_inches="tight")

    return out_filename


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

    logger = logging.getLogger(__name__)
    fill_location_lookup_db(logger)
