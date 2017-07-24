while(cursor.forward()):
  isValidRoute = list()
  for i in range(0, len(rels_list), step=2): # i step is 2 because we consider pair of only (HAS_FLIGHT->TO) and not (TO->HAS_FLIGHT)
    if departure_month==month and departure_day==day: # if the first HAS_FLIGHT is departing on the same day specified 
      haveSameDepartime = rels_list[i]['departure_time'] == rels_list[i+1]['departure_time'] 
      j = i + 1   
      if not haveSameDepartime: # Invalid route: contains segments dont have the same departure_time for :HAS_FLIGHT and :TO
        isValidRoute.append(False)
        break # quit the for loop, move to next walkable
      else:
        if len(rels_list)==2: # if non stop trip
          appendFlightInfo(price, duration)
          isValidRoute.append(True)   
        else: # at least 1 stop route 
          try:
            leg_2_departure_time = rels_list[j+1]['departure_time']
            leg_1_arrival_time   = rels_list[j]['arrival_time']
            transfer_time  = leg_2_departure_time - leg_1_arrival_time
            if (transfer_time > 30): # if the following flight is later than the arrival and the waiting time is > 30min
              transfer_time_list.append(transfer_time)
              appendFlightInfo(price, duration)
              isValidRoute.append(True)
            else: # Invalid route: contains segments with departure time earlier than arrival time.
              isValidRoute.append(False)
              break
          except IndexError: # target reached, ie: end of leg segment nodes reached, IndexError coz i move increases 2 by 2
            appendFlightInfo(price, duration) # to get the last duration and price in the last :TO relationship

    else: # Invalid trip: flight departure day doesnt match the specified day.
      isValidRoute.append(False)
      break

  if all(isValidRoute):
    extractFlightsInfo()