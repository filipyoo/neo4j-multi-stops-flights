import matplotlib.pyplot as plt
import itertools
from py2neo import Graph, Node, Relationship, Walkable, SetView
import datetime 
import pandas as pd 
from collections import OrderedDict


def queryNeo4j(month, day, limit_routes_count, origin, destination, desired_stop_count):
	neoGraph = Graph()
	query = '''
		match (m:Month{month:{month}})-[:HAS_DAY]->(d:Day{day:{day}})-[:HAS_AIRPORT]->(s:Airport{airportName:{origin}})
		match (t:Airport{airportName:{destination}, month:{month}, day:{day}})
		with m,d,s,t
		call apoc.algo.allSimplePaths(s,t,'>',{maxNodes})
		yield path as p
		return nodes(p) as nodes, relationships(p) as rels limit {limit_routes_count}
	'''

	results = neoGraph.run(query, month=month, day=day, limit_routes_count=limit_routes_count, destination=destination, origin=origin, maxNodes=desired_stop_count*2+2)
	return results


def writeDurationPriceObjectives(total_duration_valid, total_price_valid, objectives_output_file):
	# output the duration and price objectives for pareto
	total_duration_serie = pd.Series(total_duration_valid)
	total_price_serie = pd.Series(total_price_valid)
	df = pd.DataFrame()
	df[0] = total_duration_serie
	df[1] = total_price_serie
	df.to_csv(objectives_output_file, index=None, header=None, sep=' ')


def findValidRoutes(month, day, limit_routes_count, origin, destination, desired_stop_count, route_info_output_file, objectives_output_file):
	# output individual files for 0, 1, 2, 3 stops for route_info and objectives (total_duration and total_price)
	route_index          = 0
	possible_route_count = 0 
	valid_route_indexes  = []
	total_distance_valid = []
	total_duration_valid = []
	total_price_valid    = []
	all_route_info_df 	 = pd.DataFrame(columns=['airport_list', 'flight_list', 'departure_time_list', 'arrival_time_list', 'transfer_time_list', 'seat_list', 'price_list', 'distance_list', 'duration_list', 'transfer_count'])

	results = queryNeo4j(month, day, limit_routes_count, origin, destination, desired_stop_count)

	while (results.forward()):
		isValidRoute         = []
		possible_route_count = possible_route_count + 1
		transfer_time        = 0
		transfer_time_list   = []
		transfer_count       = 0 
		price_list 			 = []
		distance_list		 = []
		duration_list		 = []
		route_index          = route_index + 1
		print ('\n\nRoute {}. ====='.format(route_index))


		nodes     = dict(results.current())['nodes']
		rels      = dict(results.current())['rels']
		rels_list = [dict(r) for r in rels] # get rels properties as a dict
		nodes_list = [dict(n) for n in nodes] # get rels properties as a dict



		for i in range(0, len(rels_list), 2): # i step is 2 because we consider pair of only (HAS_FLIGHT, TO) and not (TO, HAS_FLIGHT)
			departure_month = int(datetime.datetime.strptime(rels_list[0]['departure_time'], '%Y-%m-%d %H:%M:%S').strftime('%#m'))
			departure_day   = int(datetime.datetime.strptime(rels_list[0]['departure_time'], '%Y-%m-%d %H:%M:%S').strftime('%#d'))
			if departure_month==month and departure_day==day: # if the first HAS_FLIGHT is departing on the same day specified 
				haveSameDepartime = rels_list[i]['departure_time'] == rels_list[i+1]['departure_time'] 
				j = i + 1	

				if not haveSameDepartime:
					print ('Invalid route: contains segments dont have the same departure_time for :HAS_FLIGHT and :TO.')
					isValidRoute.append(False)
					break # quit the for loop, move to next route
				else:
					if len(rels_list)==2:
						price_list.append(rels_list[j]['price'])
						distance_list.append(rels_list[j]['distance'])
						duration_list.append(rels_list[j]['duration'])
						isValidRoute.append(True)	
					else: # at least 1 stop route 
						try:
							leg_departure_time = datetime.datetime.strptime(rels_list[j+1]['departure_time'], '%Y-%m-%d %H:%M:%S')
							leg_arrival_time   = datetime.datetime.strptime(rels_list[j]['arrival_time'], '%Y-%m-%d %H:%M:%S')
							if (leg_departure_time > leg_arrival_time):
								if int((leg_departure_time - leg_arrival_time).seconds/60) > 30: # if the following flight is later than the arrival and the waiting time is > 30min
									transfer_time  = int((leg_departure_time - leg_arrival_time).seconds/60)
									transfer_time_list.append(transfer_time)
									transfer_count = transfer_count + 1
									price_list.append(rels_list[j]['price'])
									distance_list.append(rels_list[j]['distance'])
									duration_list.append(rels_list[j]['duration'])
									isValidRoute.append(True)
								else: # departure later than arrival time but not enough transfer time
									print ('Invalid route : transfer minute < 30 minutes.')
									isValidRoute.append(False)
									break
							else:
								print ('Invalid route: contains segments with departure time earlier than arrival time.')
								isValidRoute.append(False)
								break
						except IndexError: # target reached, ie: end of leg segment nodes reached, IndexError coz i move increases 2 by 2
							price_list.append(rels_list[j]['price'])  # to get the last price in the :TO segment
							distance_list.append(rels_list[j]['distance'])  # to get the last distance in the :TO segment
							duration_list.append(rels_list[j]['duration'])  # to get the last duration in the :TO segment
							pass
			else: # route with different departure day
				print ('Invalid route: flight departure day doesnt match the specified day.')
				# print (rels_list[0]['departure_time'])
				isValidRoute.append(False)
				break


		if set(isValidRoute)=={True}: # all segments contains only True valid segment 
			valid_route_indexes.append(route_index)
			total_price    = sum(price_list)
			total_distance = sum(distance_list) + sum([int(t*600/60) for t in transfer_time_list])
			total_duration = sum(duration_list) + sum(transfer_time_list)
			total_price_valid.append(total_price)
			total_distance_valid.append(total_distance)
			total_duration_valid.append(total_duration)
			
			airport_list = [a['airportName'] for a in nodes_list[0::2]] # all element at even position
			flight_list  = [f['flightCode'] for f in nodes_list[1::2]]  # all element at odd position 

			departure_time_list = [d['departure_time'] for d in rels_list[1::2]]
			arrival_time_list   = [a['arrival_time'] for a in rels_list[1::2]]
			seat_list		    = [a['seat'] for a in rels_list[1::2]]

			route_info = OrderedDict()
			route_info = {
				# 'route_index': int(route_index),
				'airport_list': ','.join(airport_list),
				'flight_list': ','.join(flight_list),
				'departure_time_list': ','.join(departure_time_list),
				'arrival_time_list': ','.join(arrival_time_list),
				'transfer_time_list': ','.join(map(str, transfer_time_list)),
				'seat_list': ','.join(seat_list),
				'price_list': ','.join(map(str, price_list)),
				'distance_list': ','.join(map(str, distance_list)),
				'duration_list': ','.join(map(str, duration_list)),
				'transfer_count': int(transfer_count),
				'total_duration': total_duration,
				'total_price': total_price
			}

			route_info_df = pd.DataFrame.from_dict([route_info])			
			all_route_info_df = pd.concat([all_route_info_df, route_info_df], axis=0)

			print(route_info)

			print ('Total stops : {}'.format(transfer_count))
			plt.scatter(total_duration, total_price, label=route_index)
			plt.xlabel('Total duration')
			plt.ylabel('Total price')


			print ('List of transfer times :  = {}'.format(transfer_time_list))
			print ('List of prices :  = {}'.format(price_list))
			print ('List of distances :  = {}'.format(distance_list))
			print ('List of durations :  = {}'.format(duration_list))


	print ('\n\n\nTotal number of valid routes : {} out of {} routes found by Neo4j and limited the query results at {} routes'.format(len(valid_route_indexes), possible_route_count, limit_routes_count))
	# print ('List of valid routes indexes: ', valid_route_indexes)
	# print ([(r,d,p, du) for (r,d,p, du) in zip(valid_route_indexes, total_distance_valid, total_price_valid, total_duration_valid)])

	writeDurationPriceObjectives(total_duration_valid, total_price_valid, objectives_output_file)

	all_route_info_df.to_csv(route_info_output_file, index=None)

	# plt.show()


def combineAllObjectivesFiles():
	df = pd.DataFrame()
	for i in range(4):
		objectives_output_file = 'objectives_{}.csv'.format(str(i))
		data = pd.read_csv(objectives_output_file, header=None, delimiter=' ')
		df = pd.concat([df, data], axis=0, verify_integrity=True, ignore_index=True)
	df.to_csv('objectives_all.csv', index=None, header=None, sep=' ')


def combineAllRouteInfoFiles():
	df = pd.DataFrame()
	for i in range(4):
		route_info_file = 'route_info_{}.csv'.format(str(i))
		data = pd.read_csv(route_info_file, delimiter=',')
		df = pd.concat([df, data], axis=0, verify_integrity=True, ignore_index=True)
	df.to_csv('route_info_all.csv', index=None, sep=',')


if __name__ == '__main__':

	maxStop = 3
	for stop_count in range(maxStop+1):
		route_info_output_file = 'route_info_{}.csv'.format(str(stop_count))
		objectives_output_file = 'objectives_{}.csv'.format(str(stop_count))
		findValidRoutes(2, 10, 10000, 'PVG', 'CDG', stop_count, route_info_output_file, objectives_output_file)
	

	combineAllObjectivesFiles()
	combineAllRouteInfoFiles()