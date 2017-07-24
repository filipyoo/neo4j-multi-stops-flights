from py2neo import Graph, Node, Relationship
from pymysql_connector import *
import datetime
import pandas as pd
from pricing import fetchallData

def createDayNodes(graph):
	graph.run("CREATE INDEX ON :Month(month)")
	graph.run("CREATE INDEX ON :Day(day)")
	query = '''
	WITH range(1,12) as months
	FOREACH(month IN months | 
	    CREATE (m:Month {month: month})
		FOREACH(day IN (CASE 
		                    WHEN month IN [1,3,5,7,8,10,12] THEN range(1,31) 
		                    WHEN month = 2 THEN range(1,28)
	                    ELSE range(1,30)
						END) |      
		    CREATE (d:Day {day: day})
			MERGE (m)-[:HAS_DAY]->(d)))
	'''
	graph.run(query)
	print ("Done creating day nodes.")


def createNonStopNoIboundRoute(graph):
	graph.run("CREATE INDEX ON :Flight(flightCode)")
	graph.run("CREATE INDEX ON :Airport(airportName)")
	data = fetchallData("SELECT * FROM pricing.data_null_0")
	query = ''' 
		MERGE (orig:Airport {airportName: {orig}, month: {month}, day:{day}})
		MERGE (dest:Airport {airportName: {dest}, month: {month}, day:{day}})
		MERGE (f:Flight{flightCode:{flightCode}, month: {month}, day:{day}})
		MERGE (m:Month{month:{month}})-[:HAS_DAY]->(d:Day{day:{day}})
		WITH orig, dest, f, m, d
		CREATE UNIQUE (m)-[:HAS_DAY]->(d)-[:HAS_AIRPORT{month: {month}, day:{day}}]->(orig)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time}}]->(f)-[:TO{month:{month}, day:{day}, departure_time: {departure_time}, arrival_time:{arrival_time}, seat: {seat}, price: {price}, duration: {duration}, distance: {distance}}]->(dest)
	'''
	i = 0
	for row in data:
		parameters = {
			'month': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#m')),
			'day': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#d')),
			'orig': row['departureairports_out - Split 1'], 
			'dest': row['arrivalairports_out - Split 1'], 
			'seat': row['outboundcabingroup - Split 1'], 
			'flightCode': row['outboundflightgroup - Split 1'], 
			'price': int(row['saleprice (MEDIAN)']), 
			'duration': row['everysegmenttotalflightminutes_out'],
			'distance': row['distance'],
			'departure_time': row['departuretimes_out - Split 1'],
			'arrival_time': row['arrivaltimes_out - Split 1'],
		}
		try:
			graph.run(query, parameters)
		except Exception:
			print ('Redudant pattern ignored.')
		print ("{} -> {} at {} Yuan, flight code {} duration={}, seat={}, departed at {} and arrived at : {} on 2017/{}/{}. === {}".format(parameters['orig'], parameters['dest'], parameters['price'], parameters['flightCode'], parameters['duration'], parameters['seat'], parameters['departure_time'], parameters['arrival_time'], parameters['month'], parameters['day'], i))
		i = i + 1


def createOneStopNoIboundRoute(graph):
	graph.run("CREATE INDEX ON :Flight(flightCode)")
	graph.run("CREATE INDEX ON :Airport(airportName)")
	data = fetchallData("SELECT * FROM pricing.data_null_1")
	query = ''' 
		MERGE (orig:Airport {airportName: {orig}, month: {month}, day:{day}})
		MERGE (dest:Airport {airportName: {dest}, month: {month}, day:{day}})
		MERGE (transfer:Airport {airportName: {transfer}, month: {month}, day:{day}})
		MERGE (f1:Flight{flightCode:{flightCode1}, month: {month}, day:{day}})
		MERGE (f2:Flight{flightCode:{flightCode2}, month: {month}, day:{day}})
		MERGE (m:Month{month:{month}})-[:HAS_DAY]->(d:Day{day:{day}})
		WITH orig, dest, transfer, m, d, f1, f2
		CREATE UNIQUE (m)-[:HAS_DAY]->(d)-[:HAS_AIRPORT{month: {month}, day:{day}}]->(orig)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time1}}]->(f1)-[:TO{month:{month}, day:{day}, departure_time: {departure_time1}, arrival_time:{arrival_time1}, seat: {seat1}, price: {price1}, duration: {duration1}, distance: {distance1}}]->(transfer)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time2}}]->(f2)-[:TO{month:{month}, day:{day}, departure_time: {departure_time2}, arrival_time:{arrival_time2}, seat: {seat2}, price: {price2}, duration: {duration2}, distance: {distance2}}]->(dest) 
	'''
	i = 0
	for row in data:
		parameters = {
			'month': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#m')),
			'day': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#d')),
			'orig': row['departureairports_out - Split 1'], 
			'dest': row['arrivalairports_out - Split 2'], 
			'transfer': row['departureairports_out - Split 2'],
			'seat1': row['outboundcabingroup - Split 1'], 
			'seat2': row['outboundcabingroup - Split 2'], 
			'flightCode1': row['outboundflightgroup - Split 1'], 
			'flightCode2': row['outboundflightgroup - Split 2'], 
			'price1': row['time_price_1'], 
			'price2': row['time_price_2'], 
			'duration1': row['real_duration_1'],
			'duration2': row['real_duration_2'],
			'departure_time1': row['departuretimes_out - Split 1'],
			'arrival_time1': row['arrivaltimes_out - Split 1'],
			'departure_time2': row['departuretimes_out - Split 2'],
			'arrival_time2': row['arrivaltimes_out - Split 2'],
		}
		try:
			graph.run(query, parameters)
		except Exception:
			print ('Redudant pattern ignored.')
		print ("{}->{}->{} on 2017/{}/{} price1={} price2{} depart= {} .... arrival={}. ===={}".format(parameters['orig'], parameters['transfer'], parameters['dest'], parameters['month'], parameters['day'], parameters['price1'], parameters['price2'], parameters['departure_time1'], parameters['arrival_time2'], i))
		i = i + 1


def createTwoStopNoIboundRoute(graph):
	graph.run("CREATE INDEX ON :Flight(flightCode)")
	graph.run("CREATE INDEX ON :Airport(airportName)")
	data = fetchallData("SELECT * FROM pricing.data_null_2")
	query = ''' 
		MERGE (orig:Airport {airportName: {orig}, month: {month}, day:{day}})
		MERGE (dest:Airport {airportName: {dest}, month: {month}, day:{day}})
		MERGE (transfer1:Airport {airportName: {transfer1}, month: {month}, day:{day}})
		MERGE (transfer2:Airport {airportName: {transfer2}, month: {month}, day:{day}})
		MERGE (f1:Flight{flightCode:{flightCode1}, month: {month}, day:{day}})
		MERGE (f2:Flight{flightCode:{flightCode2}, month: {month}, day:{day}})
		MERGE (f3:Flight{flightCode:{flightCode3}, month: {month}, day:{day}})
		MERGE (m:Month{month:{month}})-[:HAS_DAY]->(d:Day{day:{day}})
		WITH orig, dest, transfer1, transfer2, m, d, f1, f2, f3
		CREATE UNIQUE (m)-[:HAS_DAY]->(d)-[:HAS_AIRPORT{month: {month}, day:{day}}]->(orig)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time1}}]->(f1)-[:TO{month:{month}, day:{day}, departure_time: {departure_time1}, arrival_time:{arrival_time1}, seat: {seat1}, price: {price1}, duration: {duration1}, distance: {distance1}}]->(transfer1)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time2}}]->(f2)-[:TO{month:{month}, day:{day}, departure_time: {departure_time2}, arrival_time:{arrival_time2}, seat: {seat2}, price: {price2}, duration: {duration2}, distance: {distance2}}]->(transfer2)-[:HAS_FLIGHT{month:{month}, day:{day}, departure_time:{departure_time3}}]->(f3)-[:TO{month:{month}, day:{day}, departure_time: {departure_time3}, arrival_time:{arrival_time3}, seat: {seat3}, price: {price3}, duration: {duration3}, distance: {distance3}}]->(dest)
	'''
	i = 0
	for row in data:
		parameters = {
			'month': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#m')),
			'day': int(datetime.datetime.strptime(row['departuretimes_out - Split 1'], '%Y-%m-%d %H:%M:%S').strftime('%#d')),
			'orig': row['departureairports_out - Split 1'], 
			'dest': row['arrivalairports_out - Split 3'], 
			'transfer1': row['departureairports_out - Split 2'],
			'transfer2': row['departureairports_out - Split 3'],
			'seat1': row['outboundcabingroup - Split 1'], 
			'seat2': row['outboundcabingroup - Split 2'], 
			'seat3': row['outboundcabingroup - Split 3'], 
			'flightCode1': row['outboundflightgroup - Split 1'], 
			'flightCode2': row['outboundflightgroup - Split 2'], 
			'flightCode3': row['outboundflightgroup - Split 3'], 
			'price1': row['time_price_1'], 
			'price2': row['time_price_2'], 
			'price3': row['time_price_3'], 
			'duration1': row['real_duration_1'],
			'duration2': row['real_duration_2'],
			'duration3': row['real_duration_3'],
			'departure_time1': row['departuretimes_out - Split 1'],
			'arrival_time1': row['arrivaltimes_out - Split 1'],
			'departure_time2': row['departuretimes_out - Split 2'],
			'arrival_time2': row['arrivaltimes_out - Split 2'],
			'departure_time3': row['departuretimes_out - Split 3'],
			'arrival_time3': row['arrivaltimes_out - Split 3'],
		}
		try:
			graph.run(query, parameters)
		except Exception:
			print ('Redudant pattern ignored.')
		print ("{}->{}->{}->{} on 2017/{}/{} price1={} price2={} price3={} depart={} .... arrival={}. ===={}".format(parameters['orig'], parameters['transfer1'], parameters['transfer2'], parameters['dest'], parameters['month'], parameters['day'], parameters['price1'], parameters['price2'], parameters['price3'], parameters['departure_time1'], parameters['arrival_time3'], i))
		i = i + 1


if __name__ == '__main__':
	graph = Graph()
	createDayNodes(graph)
	createNonStopNoIboundRoute(graph)
	createOneStopNoIboundRoute(graph)
	createTwoStopNoIboundRoute(graph)
