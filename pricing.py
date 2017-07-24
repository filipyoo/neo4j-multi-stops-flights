from py2neo import Graph, Node, Relationship
from pymysql_connector import *
import datetime
import pandas as pd
from durationDistance import computeDuration


def fetchallData(sql_query):
	connection = dbConnexion()
	with connection.cursor() as cursor:
		cursor.execute(sql_query)
		result = cursor.fetchall()
	return result


def fetchOneData(sql_query, table):
	connection = dbConnexion()
	with connection.cursor() as cursor:
		cursor.execute(sql_query)
		result = cursor.fetchone()
	return result


def updateMySQLPriceNoStop():
	data_null_0 = 'pricing.data_null_0'
	sql_query = "SELECT * FROM " + data_null_0
	i = 1
	connection = dbConnexion()	
	with connection.cursor() as cursor:
		cursor.execute(sql_query)
		for row in cursor.fetchall():
			distance = distanceFromTwoAirports(row['departureairports_out - Split 1'], row['arrivalairports_out - Split 1'])
			sql_query = "UPDATE pricing.data_null_0 SET distance=%s WHERE id=%s"
			cursor.execute(sql_query, (distance, i))
			print (i)
			i = i + 1


def updateMySQLPriceOneStop():
	data_null_1 = 'pricing.data_null_1'
	sql_query = "SELECT * FROM " + data_null_1
	i = 1
	connection = dbConnexion()	
	with connection.cursor() as cursor:
		cursor.execute(sql_query)
		for row in cursor.fetchall():

			real_duration_1 = computeDuration(row['departureairports_out - Split 1'], row['departureairports_out - Split 2'], row['departuretimes_out - Split 1'], row['arrivaltimes_out - Split 1'])
			real_duration_2 = computeDuration(row['departureairports_out - Split 2'], row['arrivalairports_out - Split 2'], row['departuretimes_out - Split 2'], row['arrivaltimes_out - Split 2'])

			time_price_1 = real_duration_1*int(row['saleprice (MEDIAN)'])/(real_duration_1+real_duration_2)
			time_price_2 = real_duration_2*int(row['saleprice (MEDIAN)'])/(real_duration_1+real_duration_2)

			sql_query = "UPDATE pricing.data_null_1 SET real_duration_1=%s, real_duration_2=%s, time_price_1=%s, time_price_2=%s WHERE id=%s"
			cursor.execute(sql_query, (real_duration_1, real_duration_2, time_price_1, time_price_2, i))

			print (i)
			i = i + 1


def updateMySQLPriceTwoStop():
	data_null_2 = 'pricing.data_null_2'
	sql_query = "SELECT * FROM " + data_null_2
	i = 1
	connection = dbConnexion()	
	with connection.cursor() as cursor:
		cursor.execute(sql_query)
		for row in cursor.fetchall():

			real_duration_1 = computeDuration(row['departureairports_out - Split 1'], row['departureairports_out - Split 2'], row['departuretimes_out - Split 1'], row['arrivaltimes_out - Split 1'])
			real_duration_2 = computeDuration(row['departureairports_out - Split 2'], row['departureairports_out - Split 3'], row['departuretimes_out - Split 2'], row['arrivaltimes_out - Split 2'])
			real_duration_3 = computeDuration(row['departureairports_out - Split 3'], row['arrivalairports_out - Split 3'], row['departuretimes_out - Split 3'], row['arrivaltimes_out - Split 3'])

			time_price_1 = real_duration_1*int(row['saleprice (MEDIAN)'])/(real_duration_1+real_duration_2+real_duration_3)
			time_price_2 = real_duration_2*int(row['saleprice (MEDIAN)'])/(real_duration_1+real_duration_2+real_duration_3)
			time_price_3 = real_duration_3*int(row['saleprice (MEDIAN)'])/(real_duration_1+real_duration_2+real_duration_3)

			sql_query = "UPDATE pricing.data_null_2 SET real_duration_1=%s, real_duration_2=%s, real_duration_3=%s, time_price_1=%s, time_price_2=%s, time_price_3=%s WHERE id=%s"

			cursor.execute(sql_query, (real_duration_1, real_duration_2, real_duration_3, time_price_1, time_price_2, time_price_3, i))
			print (i)
			i = i + 1

			print (real_duration_1, real_duration_2, real_duration_3)


if __name__ == '__main__':
	updateMySQLPriceNoStop()
	updateMySQLPriceOneStop()
	updateMySQLPriceTwoStop()
