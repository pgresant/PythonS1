import geopandas as gpd
import pandas as pd
# import folium
# import mapclassify
# import matplotlib
# import matplotlib.pyplot as plt

def get_gdf(df):
    urlGeojson = "https://www.data.gouv.fr/fr/datasets/r/90b9341a-e1f7-4d75-a73c-bbc010c7feeb" 
    geo = gpd.read_file(urlGeojson)
    geo['code_dep'] = geo['code'].astype(str).str.zfill(2)

    gdf = gpd.GeoDataFrame(pd.merge(df, geo, on='code_dep', how='left'))
    return gdf
