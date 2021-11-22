from typing import Dict, List, Any, TypedDict

# Actual Processed Datatype of NDBC (National Data Buoy Center) Station 
class NDBCStation(TypedDict):
    id: str
    latitude: float
    longitude: float
    # elev: float
    name: str
    owner: str
    program: str
    type: str
    met: bool
    currents: bool
    water_quality: bool
    dart: bool

# Input Datatype of NDBC (National Data Buoy Center) Station 
class NDBCSourceStation(TypedDict):
    id: str
    lat: str
    lon: str
    # elev: str
    name: str
    owner: str
    pgm: str
    type: str
    met: str
    currents: str
    waterquality: str
    dart: str

# TODO add on metadata
# this is a station meta data
# OrderedDict([(
    # '@id', '21347'),
    # ('@name', '21347  320km East of Iwate, Japan'),
    # ('@owner', 'Japanese Meteorological Agency'),
    # ('@pgm', 'Tsunami'),
    # ('@type', 'dart'),
    # ('history', 
        # OrderedDict([(
            # '@start', '2012-10-09'),
            # ('@stop', '2016-09-01'),
            # ('@lat', '39.601'),
            # ('@lng', '145.799'),
            # ('@elev', ''),
            # ('@met', 'n'),
            # ('@hull', ''),
            # ('@anemom_height', '')


SourceStationConversion = {
    '@id': 'id',
    '@lat': 'latitude',
    '@lon': 'longitude',
    '@name': 'name',
    '@owner': 'owner',
    '@pgm': 'program',
    '@type': 'type',
    '@met': 'met',
    '@currents': 'currents',
    '@waterquality': 'water_quality',
    '@dart': 'dart',
}

class SourceStandardMeteorologicalData(TypedDict):
    WDIR: str # Wind direction (the direction the wind is coming from in degrees clockwise from true N) during the same period used for WSPD. See Wind Averaging Methods
    WSPD: str # Wind speed (m/s) averaged over an eight-minute period for buoys and a two-minute period for land stations. Reported Hourly. See Wind Averaging Methods.
    GST: str # Peak 5 or 8 second gust speed (m/s) measured during the eight-minute or two-minute period. The 5 or 8 second period can be determined by payload, See the Sensor Reporting, Sampling, and Accuracy section.
    WVHT: str # Significant wave height (meters) is calculated as the average of the highest one-third of all of the wave heights during the 20-minute sampling period. See the Wave Measurements section.
    DPD: str # Dominant wave period (seconds) is the period with the maximum wave energy. See the Wave Measurements section.
    APD: str # Average wave period (seconds) of all waves during the 20-minute period. See the Wave Measurements section.
    MWD: str # The direction from which the waves at the dominant period (DPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees. See the Wave Measurements section.
    PRES: str # Sea level pressure (hPa). For C-MAN sites and Great Lakes buoys, the recorded pressure is reduced to sea level using the method described in NWS Technical Procedures Bulletin 291 (11/14/80). ( labeled BAR in Historical files)
    ATMP: str # Air temperature (Celsius). For sensor heights on buoys, see Hull Descriptions. For sensor heights at C-MAN stations, see C-MAN Sensor Locations
    WTMP: str # Sea surface temperature (Celsius). For buoys the depth is referenced to the hull's waterline. For fixed platforms it varies with tide, but is referenced to, or near Mean Lower Low Water (MLLW).
    DEWP: str # Dewpoint temperature taken at the same height as the air temperature measurement.
    VIS: str # Station visibility (nautical miles). Note that buoy stations are limited to reports from 0 to 1.6 nmi.
    # PTDY: str # Pressure Tendency is the direction (plus or minus) and the amount of pressure change (hPa)for a three hour period ending at the time of observation. (not in Historical files)
    TIDE: str # The water level in feet above or below Mean Lower Low Water (MLLW).
    # Date time is UTC, which is good because finding local time of each station via the time zone would be very annoying.
    YY: str
    MM: str
    DD: str
    hh: str
    mm: str

# {"datetime_utc":"2012-07-01T15:55:00.000Z","field":"wind_direction","val":130.0,"id":"0y2w3","latitude":"44.794","longitude":"-87.313","name":"Sturgeon Bay CG Station, WI","owner":"U.S.C.G. Marine Reporting Stations","program":"IOOS Partners","type":"fixed","met":"False","currents":"False","water_quality":"False","dart":"False"}
class StandardMeteorologicalData(TypedDict):
    wind_direction: float # Wind direction (the direction the wind is coming from in degrees clockwise from true N) during the same period used for WSPD. See Wind Averaging Methods
    wind_speed: float # Wind speed (m/s) averaged over an eight-minute period for buoys and a two-minute period for land stations. Reported Hourly. See Wind Averaging Methods.
    gust: float # Peak 5 or 8 second gust speed (m/s) measured during the eight-minute or two-minute period. The 5 or 8 second period can be determined by payload, See the Sensor Reporting, Sampling, and Accuracy section.
    wave_height: float # Significant wave height (meters) is calculated as the average of the highest one-third of all of the wave heights during the 20-minute sampling period. See the Wave Measurements section.
    dominant_period: float # Dominant wave period (seconds) is the period with the maximum wave energy. See the Wave Measurements section.
    average_period: float # Average wave period (seconds) of all waves during the 20-minute period. See the Wave Measurements section.
    dominant_direction: float # The direction from which the waves at the dominant period (DPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees. See the Wave Measurements section.
    pressue: float # Sea level pressure (hPa). For C-MAN sites and Great Lakes buoys, the recorded pressure is reduced to sea level using the method described in NWS Technical Procedures Bulletin 291 (11/14/80). ( labeled BAR in Historical files)
    air_temp: float # Air temperature (Celsius). For sensor heights on buoys, see Hull Descriptions. For sensor heights at C-MAN stations, see C-MAN Sensor Locations
    water_temp: float # Sea surface temperature (Celsius). For buoys the depth is referenced to the hull's waterline. For fixed platforms it varies with tide, but is referenced to, or near Mean Lower Low Water (MLLW).
    dewpoint_temp: float # Dewpoint temperature taken at the same height as the air temperature measurement.
    visibility: float # Station visibility (nautical miles). Note that buoy stations are limited to reports from 0 to 1.6 nmi.
    # pressure_tendency: float # Pressure Tendency is the direction (plus or minus) and the amount of pressure change (hPa)for a three hour period ending at the time of observation. (not in Historical files)
    tide: float # The water level in feet above or below Mean Lower Low Water (MLLW).
    datetime_utc: str # UTC time in ISO format which is 2012-07-01T20:25:00.000Z
    field: str
    val: float

StandardMeteorologicalDataConversionPost2006 = {
    'WDIR': 'wind_direction',
    'WSPD': 'wind_speed',
    'GST': 'gust',
    'WVHT': 'wave_height',
    'DPD': 'dominant_period',
    'APD': 'average_period',
    'MWD': 'dominant_direction',
    'PRES': 'pressure',
    'ATMP': 'air_temp',
    'WTMP': 'wave_temp',
    'DEWP': 'dewpoint_temp',
    'VIS': 'visibility',
    # 'PTDY': 'pressure_tendency',
    'TIDE': 'tide',
}

StandardMeteorologicalDataConversionPre2006 = {
    'WD': 'wind_direction',
    'WSPD': 'wind_speed',
    'GST': 'gust',
    'WVHT': 'wave_height',
    'DPD': 'dominant_period',
    'APD': 'average_period',
    'MWD': 'dominant_direction',
    'BAR': 'pressure',
    'ATMP': 'air_temp',
    'WTMP': 'wave_temp',
    'DEWP': 'dewpoint_temp',
    'VIS': 'visibility',
    # 'PTDY': 'pressure_tendency',
    # This exists post 2000 but its more annoying to keep.
    # 'TIDE': 'tide',
}