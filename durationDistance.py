from py2neo import Graph, Node, Relationship
from pymysql_connector import *
import datetime
import pandas as pd
from math import radians, cos, sin, asin, sqrt
import pendulum 


def haversine(lat1, lon1, lat2, lon2): 
	# compute distance between two airports based on their latitude and longitude
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6367 # km
    return r * c  


def distanceFromTwoAirports(airportName1, airportName2):
	df = pd.read_csv('openFlights.csv')
	try:
		lat1 = df.loc[df['ICAO3']==airportName1, 'Latitude'].values[0]
		lon1 = df.loc[df['ICAO3']==airportName1, 'Longtitude'].values[0]
		lat2 = df.loc[df['ICAO3']==airportName2, 'Latitude'].values[0]
		lon2 = df.loc[df['ICAO3']==airportName2, 'Longtitude'].values[0]
		distance = int(haversine(lat1, lon1, lat2, lon2))
	except Exception:
		print ("Cannot compute distance between {} and {}.".format(airportName1, airportName2))
		distance = 999999999
	return distance


def computeDuration(source, destination, departure_time, arrival_time):
	df  = pd.read_csv('openFlights.csv')
	tz1 = df.loc[df['ICAO3']==source, 'timezone'].values[0]
	tz2 = df.loc[df['ICAO3']==destination, 'timezone'].values[0]
	
	departure_time = datetime.datetime.strptime(departure_time, '%Y-%m-%d %H:%M:%S')
	d_year         = int(departure_time.strftime('%#Y'))
	d_month        = int(departure_time.strftime('%#m'))
	d_day          = int(departure_time.strftime('%#d'))
	d_hour         = int(departure_time.strftime('%#H'))
	d_min          = int(departure_time.strftime('%#M'))
	d_sec          = int(departure_time.strftime('%#S'))
	
	arrival_time   = datetime.datetime.strptime(arrival_time, '%Y-%m-%d %H:%M:%S')
	a_year         = int(arrival_time.strftime('%#Y'))
	a_month        = int(arrival_time.strftime('%#m'))
	a_day          = int(arrival_time.strftime('%#d'))
	a_hour         = int(arrival_time.strftime('%#H'))
	a_min          = int(arrival_time.strftime('%#M'))
	a_sec          = int(arrival_time.strftime('%#S'))


	dt_source      = pendulum.create(d_year, d_month, d_day, d_hour, d_min, d_sec, tz=tz1)
	dt_destination = pendulum.create(a_year, a_month, a_day, a_hour, a_min, a_sec, tz=tz2)

	duration = dt_source.diff(dt_destination).in_minutes()
	return duration