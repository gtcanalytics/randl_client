import numpy as np
import pandas as pd
from datetime import datetime, timedelta
#import pyproj
from sklearn.preprocessing import MinMaxScaler
from . import constant_vars

transformer_to_geocent = pyproj.Transformer.from_crs({"proj": 'latlong', "ellps": 'WGS84', "datum": 'WGS84'},{"proj": 'geocent', "ellps": 'WGS84', "datum": 'WGS84'},)
Geocentric_min_max_scaler = MinMaxScaler(feature_range=(-1,1))
Geocentric_min_max_scaler.fit_transform([[constant_vars.earth_radius*1000*-1], [constant_vars.earth_radius*1000]])
Time_min_max_scaler = MinMaxScaler(feature_range=(-1,1))
Time_min_max_scaler.fit_transform([[-15*60], [15*60]])

def load_pyproj_transformer():
    try:
        import pyproj
    except:
        return print('could not load pyproj')

    pyproj_transformer = pyproj.Transformer.from_crs(
        {"proj":'latlong', "ellps":'WGS84', "datum":'WGS84'},
        {"proj":'geocent', "ellps":'WGS84', "datum":'WGS84'},
        )
    return pyproj_transformer

def load_geocentric_min_max_scaler(v_min=constant_vars.earth_radius*1000*-1, v_max=constant_vars.earth_radius*1000):
    try:
        from sklearn.preprocessing import MinMaxScaler
    except:
        return print('could not load sklearn')
    geocentric_min_max_scaler = MinMaxScaler(feature_range=(-1,1))
    geocentric_min_max_scaler.fit_transform([[v_min], [v_max]])
    return geocentric_min_max_scaler


# Function to convert lat/lon to geocentric coordinates
def lonlat_to_geocentric(lon, lat, elev=0, transformer=transformer_to_geocent):
    X, Y, Z = transformer.transform(lon, lat, elev)
    return X, Y, Z


# Function to convert lat/lon to geocentric coordinates
def geocentric_to_lonlat(x, y, z, transformer=transformer_to_geocent):
    lon, lat, elev = transformer.transform(x, y, z, direction="INVERSE")
    return lon, lat, elev


def scale_time(time, scaler=Time_min_max_scaler):
    scaled_time = scaler.transform([[time]])[0][0]
    return scaled_time


def scale_geocentric(x, y, z, scaler=Geocentric_min_max_scaler):
    x = Geocentric_min_max_scaler.transform([[x]])[0][0]
    y = Geocentric_min_max_scaler.transform([[y]])[0][0]
    z = Geocentric_min_max_scaler.transform([[z]])[0][0]
    return x, y, z


def unscale_time(time, scaler=Time_min_max_scaler):
    scaled_time = scaler.inverse_transform([[time]])[0][0]
    return scaled_time

def unscale_geocentric(x, y, z, scaler=Geocentric_min_max_scaler):
    x = Geocentric_min_max_scaler.inverse_transform([[x]])[0][0]
    y = Geocentric_min_max_scaler.inverse_transform([[y]])[0][0]
    z = Geocentric_min_max_scaler.inverse_transform([[z]])[0][0]
    return x, y, z

def constants():
    return {"Earth radius": constant_vars.earth_radius}