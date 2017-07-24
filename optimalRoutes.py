import matplotlib.pyplot as plt
import pandas as pd
from collections import namedtuple
from scipy.spatial import distance


def plotSolutions(nondominated_solutions, objectives_file):
    plt.style.use('seaborn-darkgrid')
    label_added_1 = False
    label_added_2 = False
    data = pd.read_csv(objectives_file, header=None, delimiter=' ')
    for i in range(len(data)):
        if tuple((data.iloc[i,0], data.iloc[i,1])) in nondominated_solutions:
            if not label_added_1:
                plt.scatter(data.iloc[i,0], data.iloc[i,1], color='forestgreen', label='Optimal routes')
                label_added_1 = True
            else :
                plt.scatter(data.iloc[i,0], data.iloc[i,1], color='forestgreen')
        else : 
            if not label_added_2:
                plt.scatter(data.iloc[i,0], data.iloc[i,1], color='indianred', label='Routes')
                label_added_2 = True
            else :
                plt.scatter(data.iloc[i,0], data.iloc[i,1], color='indianred')

    plt.xlabel('Total duration (min)')
    plt.ylabel('Total price (Yuan)')
    plt.title('Duration and Price for all valid routes found.')
    plt.legend(loc='upper left')
    plt.show()


def findRouteInfo(nondominated_solutions):
	# after done finding the value for the optimal routes, find the corresponding route information (airport name, departure time(s) etc)
    all_route_info = pd.read_csv('route_info_all.csv')
    all_route_info.drop_duplicates(inplace=True)
    dominated_routes = pd.DataFrame()
    for elem in nondominated_solutions:
        same_price = all_route_info['total_price'] == elem.price
        same_duration = all_route_info['total_duration'] == elem.duration
        dominated_route = all_route_info[same_price & same_duration]
        dominated_routes = pd.concat([dominated_routes, dominated_route], axis=0, verify_integrity=True, ignore_index=True)

    dominated_routes.drop_duplicates(inplace=True)
    dominated_routes.to_csv('dominated_routes.csv', index=None)    


def findOptimalRoutes(objectives_file, top, metric):
    # find optimal solutions according to different metric : alpha_max, Euclidean distance, Mahalanobis distance 
    nondominated_solutions = []
    df                     = pd.DataFrame(columns=['duration', 'price', 'alpha'])
    data                   = pd.read_csv(objectives_file, header=None, delimiter=' ')
    optimal_duration       = min(data[0])
    optimal_price          = min(data[1])
    optimal_vector         = [(optimal_duration, optimal_price)]

    if metric=='alpha_max':
        alpha = [max(data.iloc[i,0]/optimal_duration, data.iloc[i,1]/optimal_price) - 1 for i in range(len(data))]
    elif metric=='euclidean':
        alpha = [distance.euclidean(optimal_vector, (data.iloc[i,0], data.iloc[i,1])) for i in range(len(data))]
    elif metric=='mahalanobis':
        solutions_list = [(data.iloc[i,0], data.iloc[i,1]) for i in range(len(data)) ] # tuple of (duration,price) for each valid route
        alpha = distance.cdist(solutions_list, optimal_vector, 'mahalanobis', VI=None) # dont use the inverse of the covariance matrix
        alpha = [elem[0] for elem in alpha.tolist()] # ndarray to list then flatten the list of lists
                
    df['duration'] = data[0]
    df['price'] = data[1]
    df['alpha'] = pd.Series(alpha)

    top_ranked_flights = df.sort_values('alpha').head(top)
    Nondominated_solutions = namedtuple('Nondominated_solutions', 'duration price')
    for i in range(len(top_ranked_flights)):
        nondominated_solutions.append(Nondominated_solutions(top_ranked_flights.iloc[i,0], top_ranked_flights.iloc[i,1]))
    return nondominated_solutions  # list of namedtuple of Nondominated_solutions


if __name__ == "__main__":

    nondominated_solutions = findOptimalRoutes('objectives_all.csv', top=1000, metric='mahalanobis')    
    plotSolutions(nondominated_solutions, 'objectives_all.csv')
    findRouteInfo(nondominated_solutions)