import time
import logging
import math
from PIL import Image
import numpy as np
import pandas as pd
import geopandas
import geopy
from geopy.geocoders import Nominatim, MapBox
from geopy.extra.rate_limiter import RateLimiter
import json
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import scrape_new_hosts as host_scraper

import mpld3
#mpld3 hack
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumpyEncoder, self).default(obj)
from mpld3 import _display
_display.NumpyEncoder = NumpyEncoder

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


def make_custom_marker(text, flip_y=False):
    from matplotlib.path import Path
    from matplotlib.textpath import TextPath
    from matplotlib.font_manager import FontProperties

    textPath = TextPath((0,4), text, size=3)
    textPath = textPath.transformed(mpl.transforms.Affine2D().translate(-1 * len(text),0))
    circle = Path.unit_circle()
    triangle = Path([[0,0],[1,0],[0.5,0.5],[0,0]],
            [Path.MOVETO, Path.LINETO, Path.LINETO, Path.CLOSEPOLY])
    triangle = triangle.transformed(mpl.transforms.Affine2D().translate(-0.5,0).scale(3,-4).translate(0,3))

    verts = np.concatenate([circle.vertices, textPath.vertices, triangle.vertices])
    codes = np.concatenate([circle.codes, textPath.codes, triangle.codes])
    combined_marker = Path(verts, codes)

    combined_marker = combined_marker.transformed(mpl.transforms.Affine2D().scale(1000,-1000 if flip_y else 1000))

    return combined_marker


def plot_geostat_update(out_filename, timespan, save_as_html=False):
    hosts_df = host_scraper.read_hosts_from_db()
    hosts_df_timed = hosts_df[pd.notna(hosts_df["first_online_timestamp"])].copy() 
    hosts_df_timed["datetime"] = pd.to_datetime(hosts_df_timed.first_online_timestamp * 1e9)
    start_day = pd.Timestamp.today() - pd.Timedelta(days=timespan)
    hosts_df_timed = hosts_df_timed.loc[hosts_df_timed.datetime >= start_day]

    if save_as_html:
        fig_map, ax_map = plt.subplots(1, 1,figsize=(16, 8))
        fig_linegraph, ax_linegraph = plt.subplots(1, 1,figsize=(16, 4))
    else:
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
            if n>1:
                marker = make_custom_marker(str(n), flip_y=save_as_html)
                gdf.plot(ax=ax_map, color='red',marker=marker,markersize=1000,edgecolor="black")
            else:
                gdf.plot(ax=ax_map, color='red',marker=".",markersize=80,edgecolor="black")


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
    offsets = []
    for dt in r:
        df = hosts_df_timed_known_loc.loc[hosts_df_timed.datetime.dt.date == dt.date()]
        country_coded_df = df.copy()
        country_coded_df["countrycode"] = [loc.split(",")[1].strip() for loc in df.location]
        unique_countries = country_coded_df.drop_duplicates(subset='countrycode', keep="first")["countrycode"]
        for i, country in enumerate(unique_countries.tolist()):
            offset = i*(max(counts)/10.0 + max(counts)/50.0)
            if save_as_html:
                offsets.append((dt, 
                    counts[dt]-offset  if counts[dt] >= max(counts) * 0.75 else counts[dt]+offset, 
                    country.lower(), int(counts[dt] > max(counts) * 0.75)))
            else:
                if counts[dt] >= max(counts) * 0.75:
                    offset_image(dt, counts[dt]-offset, country, ax_linegraph,yoffset=-20)
                else:
                    offset_image(dt, counts[dt]+offset, country, ax_linegraph)

    if save_as_html:
        mpld3.plugins.connect(fig_map, mpld3.plugins.Zoom())
        grouped = geo_df_data.groupby('City')
        points = []; cities = []
        for city, data in grouped:
            coord = (data.Longitude.tolist()[0], data.Latitude.tolist()[0])
            points.append(coord); cities.append(city)
        ## pseudo-transparent scatter
        scatter = ax_map.scatter(list(zip(*points))[0], list(zip(*points))[1], s=40, alpha=.01, marker='s', edgecolor='none')
        tooltip = mpld3.plugins.PointLabelTooltip(scatter, labels=cities)
        mpld3.plugins.connect(fig_map, tooltip)
        mpld3.save_html(fig_map, out_filename.split(".")[0] + ".html")
        plt.close(fig_map)
        
        #clear any plugins such as zoom
        mpld3.plugins.clear(fig_linegraph)
        ## pseudo-transparent scatter for flags
        scatter = ax_linegraph.scatter(list(zip(*offsets))[0], list(zip(*offsets))[1], s=40, alpha=.01, marker='s', edgecolor='none')
        mpld3.plugins.connect(fig_linegraph, AddImage(scatter, list(zip(*offsets))[2], list(zip(*offsets))[3]))
        mpld3.save_html(fig_linegraph, out_filename.split(".")[0] + "_chart" + ".html")
        plt.close(fig_linegraph)

    else:
        fig.savefig(out_filename, dpi=200,bbox_inches="tight")
        plt.close(fig)

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

def offset_image_barchart(coord, name, ax):
    img = get_flag(name, img_length=32)
    im = OffsetImage(img, zoom=0.72)
    im.image.axes = ax
    ab = AnnotationBbox(im, (coord, 0),  xybox=(0., -16.), frameon=False,
                        xycoords='data',  boxcoords="offset points", pad=0)
    ax.add_artist(ab)

class AddImage(mpld3.plugins.PluginBase):  # inherit from PluginBase
    """Hello World plugin"""

    JAVASCRIPT = """
    mpld3.register_plugin("AddImage", AddImage);
    AddImage.prototype = Object.create(mpld3.Plugin.prototype);
    AddImage.prototype.constructor = AddImage;
    AddImage.prototype.requiredProps = ["id", "images", "downwards_flag"];
    function AddImage(fig, props){
        mpld3.Plugin.call(this, fig, props);
    };

    AddImage.prototype.draw = function(){
        var obj = mpld3.get_element(this.props.id);
        var images = this.props.images;
        var downwards_flag = this.props.downwards_flag;
        obj.elements().map(function(node) {
            console.log(node);
            parent = d3.select(node.parentNode);
            node.map(function(e, index) {
                var img_name = images[index];
                console.log("flags/" + img_name + ".png");
                var t = d3.transform(d3.select(e).attr("transform") ).translate;
                t[0] -= 10;
                if (!downwards_flag[index]) 
                {
                    t[1] -= 25.0;                
                }

                parent.append("svg:image")
                .attr("transform", "translate(" + t[0] + "," + t[1] + ")")
                .attr('width', 20)
                .attr('height', 20)
                .attr("xlink:href",  "flags/" + img_name + ".png")
             })
        });
    }
    """
    def __init__(self, points, images, downwards_flag):
        self.dict_ = {"type": "AddImage", "id": mpld3.utils.get_id(points), 
                        "images": images, "downwards_flag": downwards_flag}

def plot_country_stat(out_filename, save_as_html=False):
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
    ax.set_ylim(0,100)
    ax.set_title("Online hosts per country")
    offsets = []
    for idx, (country, count) in enumerate(country_counts.iteritems()):
        if save_as_html:
            offsets.append((idx, count+2, country.lower(), 0))
        else:
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
    if save_as_html:
        #clear any plugins such as zoom
        mpld3.plugins.clear(fig)
        ## pseudo-transparent scatter for flags
        x,y,imgs, downflag = list(zip(*offsets))
        scatter = ax.scatter(x,y, s=40, alpha=.01, marker='s', edgecolor='none')
        mpld3.plugins.connect(fig, AddImage(scatter, imgs, downflag))
        mpld3.save_html(fig, out_filename.split(".")[0] + ".html")
    else:
        fig.savefig(out_filename,dpi=200,bbox_inches="tight")
    plt.close(fig)

def plot_city_ranking(out_filename, save_as_html=False):
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
    if save_as_html:
        mpld3.save_html(fig, out_filename.split(".")[0] + ".html")
        plt.close(fig)  
        return


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


def make_cities_marker(length, flip_y=True):
    from matplotlib.path import Path
    from matplotlib.textpath import TextPath
    from matplotlib.font_manager import FontProperties

    circle = Path.unit_circle()
    scale = math.sqrt(length*0.8) / 4.0 + (0.4 if length > 1 else 0)
    circle = circle.transformed(mpl.transforms.Affine2D().scale(scale, scale))

    if length <= 1:
        verts = circle.vertices
        codes = circle.codes
    else:
        text = str(length)
        fp = FontProperties(family="DejaVu Sans", style="oblique")
        textPath = TextPath((0,0), text, size=1 if length <= 9 else 1.2 , prop=fp)
        textPath = textPath.transformed(mpl.transforms.Affine2D().scale(1,-1 if flip_y else 1)
                                            .translate(-len(text)/2.5 if len(text) > 1 else -0.2,0.3))
        verts = np.concatenate([circle.vertices, textPath.vertices])
        codes = np.concatenate([circle.codes, textPath.codes])
        
    combined_marker = Path(verts, codes)
    return combined_marker

def mscatter(x,y,ax=None, m=None, **kw):
    import matplotlib.markers as mmarkers
    sc = ax.scatter(x,y,**kw)
    if (m is not None) and (len(m)==len(x)):
        paths = []
        for marker in m:
            if isinstance(marker, mmarkers.MarkerStyle):
                marker_obj = marker
            else:
                marker_obj = mmarkers.MarkerStyle(marker)
            path = marker_obj.get_path()#.transformed(marker_obj.get_transform())
            paths.append(path)
        sc.set_paths(paths)
    return sc


def plot_interactive_stargate_hosts(out_filename, logger):
    from matplotlib.legend_handler import HandlerTuple

    hosts_df = host_scraper.read_hosts_from_db()
    stargates = host_scraper.read_stargates_from_db()
    cache_new_locations(stargates, logger)

    fig, ax_map = plt.subplots(1, 1, figsize=(16, 16))
    ax_map.axis('off')
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world  = world[(world.pop_est>0) & (world.name!="Antarctica")]
    world.plot(ax=ax_map, color="lightblue",edgecolor="#6ec4cc")
    ax_map.set_aspect('equal')
    ax_map.set_ylim(-60, 88)
    ax_map.set_xlim(-185, 185)
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
         'Longitude': merged_df.longitude,
         'Stargate': merged_df.stargate
    })

    grouped_by_stargate = geo_df_data.groupby('Stargate')
    stargate_l = []
    pseudo_points = []
    for index, (stargate, stargate_group) in enumerate(grouped_by_stargate):
        stargate_l.append(stargate)

        grouped_by_city = stargate_group.groupby('City')
        city_points_x = []
        city_points_y = []
        markers = []
        for city, group in grouped_by_city:
            city_points_x += [group.Longitude.tolist()[0]]
            city_points_y += [group.Latitude.tolist()[0]]
            markers += [make_cities_marker(len(group))]
            size = math.sqrt(len(group)*0.8) / 4.0 + (0.4 if len(group) > 1 else 0)
            pseudo_points += [(city_points_x[-1], city_points_y[-1], size * 100, city)]

        colors = ["#9ACD32", "#FF6666", "#7CFC00", "#228B22", "#FFE4B5", "#808080", "#FF8C00", "#FFFF00",  "#808000", "#FFFFFF"]
        mscatter(city_points_x, city_points_y, ax=ax_map, m=markers, 
            edgecolor="black", s=100, color=colors[index], label=stargate)

    handles, labels = ax_map.get_legend_handles_labels() # return lines and labels
    interactive_legend = mpld3.plugins.InteractiveLegendPlugin(handles,
                                                         stargate_l,
                                                         alpha_unsel=0.5,
                                                         alpha_over=1.5, 
                                                         start_visible=True)

    mpld3.plugins.connect(fig, interactive_legend)

    ## pseudo-transparent scatter
    x,y, sizes, labels = list(zip(*pseudo_points))
    scatter = ax_map.scatter(x,y, s=sizes, alpha=.01, marker='s', edgecolor='none')
    tooltip = mpld3.plugins.PointLabelTooltip(scatter, labels=labels)
    mpld3.plugins.connect(fig, tooltip)


    mpld3.save_html(fig, out_filename.split(".")[0] + ".html")
    plt.close(fig)

def gen_all_html(logger=None):
    if not logger:
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
        logger = logging.getLogger("html_generation")

    plot_geostat_update("htmltest/geostat.html", timespan=60, save_as_html=True)
    plot_city_ranking("htmltest/cityranking.html", save_as_html=True)
    plot_country_stat("htmltest/onlinestats.html", save_as_html=True)
    plot_interactive_stargate_hosts("htmltest/test_stargate_hosts.html", logger)
    

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

    logger = logging.getLogger(__name__)
    plot_country_stat("htmltest/onlinestats.html", save_as_html=True)
    # gen_all_html(logger)
    #fill_location_lookup_db(logger)
    # plot_geostat_update("htmltest/test_geostat.png", timespan=60, save_as_html=True)
    # plot_interactive_stargate_hosts("htmltest/test_stargate_hosts.html", logger)
