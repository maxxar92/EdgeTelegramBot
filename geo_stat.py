import time
import logging
from PIL import Image
import numpy as np
import pandas as pd
import geopandas
import geopy
from geopy.geocoders import Nominatim, MapBox
from geopy.extra.rate_limiter import RateLimiter
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
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
                result = geocode(city, country=countrycode)
            else:
                result = geocode({"city":city}, country_codes=countrycode)
        except Exception as e:
            logger.error("geocode processing failed with {}".format(e))
            return None
        if result is None:
            logger.warning("geocode could not find {} {}. None is used.".format(city, countrycode))
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


def get_flag(name,img_length=32):
    path = "flags/{}.png".format(name.lower())
    img = Image.open(path).convert('RGB')
    img_height = int(img_length * 1.0 / img.size[0] * img.size[1])
    img = img.resize((img_length, img_height), Image.ANTIALIAS)
    return np.asarray(img)

def offset_image(dt, ycoord, name, ax, yoffset=16):
    img = get_flag(name)
    im = OffsetImage(img, zoom=0.72)
    im.image.axes = ax

    ab = AnnotationBbox(im, (dt, ycoord),  xybox=(0., yoffset), frameon=True,
                        xycoords='data',  boxcoords="offset points", pad=0)

    ax.add_artist(ab)

def plot_geostat_update(out_filename, timespan):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df_timed = hosts_df[pd.notna(hosts_df["first_online_timestamp"])].copy() 
    hosts_df_timed["datetime"] = pd.to_datetime(hosts_df_timed.first_online_timestamp * 1e9)
    start_day = pd.Timestamp.today() - pd.Timedelta(days=timespan)
    hosts_df_timed = hosts_df_timed.loc[hosts_df_timed.datetime >= start_day]

    fig, (ax_map,ax_linegraph) = plt.subplots(2, 1,figsize=(16, 16), gridspec_kw={'height_ratios': [6, 1],"hspace":-0.5})
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

    for n in range(100):
        n_count_nodes = geo_df_data.groupby('City').filter(lambda x: len(x) == n)
        if not n_count_nodes.empty:    
            gdf = geopandas.GeoDataFrame(n_count_nodes, 
                geometry=geopandas.points_from_xy(n_count_nodes.Longitude, n_count_nodes.Latitude))    
            gdf.plot(ax=ax_map, color='red',marker=".",markersize=80,edgecolor="black")
            if n>1:
                # shift up marker slightly to align to center
                gdf_shifted1 = geopandas.GeoDataFrame(n_count_nodes.copy(), 
                    geometry=geopandas.points_from_xy(n_count_nodes.Longitude, n_count_nodes.Latitude+1.9))
                gdf_shifted1.plot(ax=ax_map, color='red',marker="v",markersize=60,edgecolor="black")
                # plot number of nodes in this city
                gdf_shifted2 = geopandas.GeoDataFrame(n_count_nodes.copy(), 
                    geometry=geopandas.points_from_xy(n_count_nodes.Longitude, n_count_nodes.Latitude+6.2))
                gdf_shifted2.plot(ax=ax_map, color='black',marker="${}$".format(n),markersize=100)


    hosts_unknown_locs = hosts_df_timed[~hosts_df_timed.host_name.isin(hosts_df_timed_known_loc.host_name)]
    if not hosts_unknown_locs.empty:
        stargate_txt = ""
        for stargate, count in hosts_unknown_locs.groupby("stargate")["stargate"].count().iteritems():
            stargate_txt += "{}={}, ".format(stargate, count)
        ax_map.text(-45,-50,"Hosts with unknown locations, \nper stargate: {}".format(stargate_txt[:-2]))

    ax_linegraph.yaxis.set_major_locator(MaxNLocator(integer=True))
    counts = hosts_df_timed.groupby([hosts_df_timed.datetime.dt.date])["datetime"].count()
    r = pd.date_range(end=pd.Timestamp.today().date(),periods=timespan)
    counts = counts.reindex(r).fillna(0.0) #fill dates without new hosts
    ax_linegraph.set_title("New hosts online - per day")
    ax_linegraph.set_ylabel("Hosts")
    counts.plot(ax=ax_linegraph,linestyle='--', marker='o',color="red",markersize=10)

    date_time_grouped = hosts_df_timed_known_loc.groupby([hosts_df_timed.datetime.dt.date])
    for dt in r:
        df = hosts_df_timed_known_loc.loc[hosts_df_timed.datetime.dt.date == dt.date()]
        country_coded_df = df.copy()
        country_coded_df["countrycode"] = [loc.split(",")[1].strip() for loc in df.location]
        unique_countries = country_coded_df.drop_duplicates(subset='countrycode', keep="first")["countrycode"]
        for i, country in enumerate(unique_countries.tolist()):
            offset = i*(max(counts)/10.0 + max(counts)/50.0)
            if counts[dt] >= max(counts) * 0.75:
                offset_image(dt, counts[dt]-offset, country, ax_linegraph,yoffset=-20)
            else:
                offset_image(dt, counts[dt]+offset, country, ax_linegraph)
    fig.savefig(out_filename, dpi=200,bbox_inches="tight")
    plt.close(fig)

def offset_image_barchart(coord, name, ax):
    img = get_flag(name, img_length=32)
    im = OffsetImage(img, zoom=0.72)
    im.image.axes = ax
    ab = AnnotationBbox(im, (coord, 0),  xybox=(0., -16.), frameon=False,
                        xycoords='data',  boxcoords="offset points", pad=0)
    ax.add_artist(ab)

def plot_country_stat(out_filename):
    hosts_df = host_scraper.read_hosts_from_db()
    loc_df = retrieve_cached_locations()
    hosts_df_known_loc = hosts_df.loc[(hosts_df.location!="-")].copy()
    hosts_df_known_loc["countrycode"] = [loc.split(",")[1].strip() for loc in hosts_df_known_loc.location]
    country_counts = hosts_df_known_loc.groupby('countrycode')["countrycode"].count().sort_values(ascending=False)
        
    fig, (ax, ax_rest) = plt.subplots(2,1, figsize=(16, 8), gridspec_kw={"hspace":0.3})
    ax.bar(range(len(country_counts)), country_counts, width=0.8,align="center")
    ax.set_xticks(range(len(country_counts)))
    ax.set_xticklabels(country_counts.index)
    ax.tick_params(axis='x', which='major', pad=26)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlim(-0.5,len(country_counts)-.5)
    ax.set_title("Online hosts per country")
    for idx, (country, count) in enumerate(country_counts.iteritems()):
        offset_image_barchart(idx, country, ax)
        

    hosts_df_unknown_loc = hosts_df.loc[(hosts_df.location=="-") & 
                                        (hosts_df.status != "Offline") & 
                                        (hosts_df.status != "Status unknown")]
    unknown_hosts_per_stargate = hosts_df_unknown_loc.groupby("stargate")["stargate"].count()

    hosts_df_offline =  hosts_df.loc[hosts_df.status == "Offline"]
    hosts_df_unknown_stat =  hosts_df.loc[hosts_df.status == "Status unknown"]

    labels = unknown_hosts_per_stargate.index.tolist() + ["Status unknown", "Offline"]
    merged_stats = unknown_hosts_per_stargate.copy()
    merged_stats.at[len(labels)-2] = hosts_df_unknown_stat.shape[0]
    merged_stats.at[len(labels)-1] = hosts_df_offline.shape[0]

    ax_rest.bar(range(len(labels)), merged_stats, width=0.4,align="center")
    ax_rest.set_xticks(range(len(labels)))
    ax_rest.set_xticklabels(labels)
    ax_rest.axvline(x=unknown_hosts_per_stargate.shape[0]-0.5,linestyle='dashed')
    ax_rest.text(0.5,max(merged_stats)/1.1, "Online hosts with unknown locations, per stargate",  fontsize=18)
    fig.savefig(out_filename,dpi=200,bbox_inches="tight")
    plt.close(fig)

def plot_city_ranking(out_filename):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df_known_loc = hosts_df.loc[(hosts_df.location!="-")].copy()
    country_counts = hosts_df_known_loc.groupby('location')["location"].count().sort_values(ascending=False)
    country_counts = country_counts.head(25)

    cell_text = []
    for row in country_counts.iteritems():
        cell_text.append(row)

    fig, ax = plt.subplots(1,1, figsize=(3,5))
    #ax.set_xlim(0,0.64)
    ax.table(cellText=cell_text, loc='best', 
        colWidths=[0.5,0.1], bbox=[-0.2, -0.2, 1.3, 1.0], 
        cellLoc='left' , edges="open")
    ax.axis('off')
    plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
    fig.tight_layout()
    fig.savefig(out_filename,dpi=200,bbox_inches="tight")#, pad_inches=0.0)
    plt.close(fig)

    # hack: the table margins are always messy and there seems to be no way to crop 
    # out the major whitespace, therefore do cropping on the saved image
    image = plt.imread(out_filename)
    h,w,c = image.shape
    img_cropped = image[180:h, 10:w-20, :]
    plt.imsave(out_filename, img_cropped)



def plot_stargate_hosts(out_filename, logger, stargate):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df = hosts_df.loc[hosts_df.stargate == stargate]

    stargates = host_scraper.read_stargates_from_db()
    cache_new_locations(stargates, logger)

    fig, ax_map = plt.subplots(1, 1, figsize=(16, 16))
    ax_map.axis('off')
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world  = world[(world.pop_est>0) & (world.name!="Antarctica")]
    ax_map.set_aspect('equal')
    loc_df = retrieve_cached_locations()
    hosts_df_known_loc = hosts_df.loc[(hosts_df.location!="-") & 
                                                  hosts_df.location.isin(loc_df.explorer_location) ]
    # equal column names needed for pd.merge
    df_renamed = hosts_df_known_loc.rename(columns={"location": "explorer_location"}) 
    merged_df = pd.merge(df_renamed, loc_df, how='inner', on=['explorer_location'])

    geo_df_data = pd.DataFrame(
        {'City': [loc.split(",")[0] for loc in merged_df.explorer_location],
         'Country': [loc.split(",")[-1] for loc in merged_df.retrieved_address],
         'Latitude': merged_df.latitude,
         'Longitude': merged_df.longitude
    })


    fetched_stargate = stargates[stargates.stargate_name == stargate]
    all_geo_nodes = geo_df_data.copy()
    plot_stargate = False
    if len(fetched_stargate) > 0 and fetched_stargate.location.isin(loc_df.explorer_location).any():
        fetched_stargate = fetched_stargate.rename(columns={"location": "explorer_location"}) 
        merged_stargate_df = pd.merge(fetched_stargate, loc_df, how='inner', on=['explorer_location'])
        city, country = merged_stargate_df.explorer_location[0].split(",")
        # make sure the stargate is included in the bounds
        all_geo_nodes.loc[len(all_geo_nodes.index)] = [city, country, merged_stargate_df.latitude[0],  merged_stargate_df.longitude[0]]
        plot_stargate = True

    # this gdf is only used for bounds calculation
    total_gdf = geopandas.GeoDataFrame(all_geo_nodes, 
            geometry=geopandas.points_from_xy(all_geo_nodes.Longitude, all_geo_nodes.Latitude))    
    minx, miny, maxx, maxy = total_gdf.total_bounds
    marginx, marginy = abs((maxx-minx)/20.0), abs((maxy-miny)/20.0)
    ax_map.set_xlim(minx-marginx, maxx+marginx)
    ax_map.set_ylim(miny-marginy, maxy+marginy)
    world.plot(ax=ax_map, color="lightblue",edgecolor="#6ec4cc")


    grouped_by_city = geo_df_data.groupby('City')#.filter(lambda x: len(x) == n)
    for city, group in grouped_by_city:
        gdf = geopandas.GeoDataFrame(group, 
            geometry=geopandas.points_from_xy(group.Longitude, group.Latitude))    
        gdf.plot(ax=ax_map, color='red',marker=".",markersize=120+60*len(group)+(200 if len(group) > 1 else 0),edgecolor="black")
        if len(group)>1:
            gdf.plot(ax=ax_map, color='black',marker="${}$".format(len(group)),markersize=50)

    if plot_stargate:
        gdf = geopandas.GeoDataFrame(merged_stargate_df, 
            geometry=geopandas.points_from_xy(merged_stargate_df.longitude, merged_stargate_df.latitude))    
        gdf.plot(ax=ax_map, color='lightgreen',marker=".",markersize=750,edgecolor="black")
        gdf.plot(ax=ax_map, color='black',marker="${}$".format(stargate),markersize=80)


    hosts_unknown_locs = hosts_df[~hosts_df.host_name.isin(hosts_df_known_loc.host_name)]
    ax_map.set_title("Hosts connected to {} (+ {} {} hosts in unknown locations)".format(stargate, len(hosts_unknown_locs), stargate))
    fig.savefig(out_filename, dpi=240,bbox_inches="tight")
    plt.close(fig)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

    logger = logging.getLogger(__name__)
    #fill_location_lookup_db(logger)
    plot_stargate_hosts("test_stargatehosts.png", logger, "lax")
