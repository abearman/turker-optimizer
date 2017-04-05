import sys
import time
import random

TEN_MINUTE_RETAINER_MILLIS = 600000
TEN_MINUTE_RETAINER_SECONDS = 600

def get_delays():
	with open("delays.tsv") as f:
		examples = f.readlines()
		headers = examples.pop(0)

		# Transform retainerRateDollars into float, retainerMillis into int,
		#  and postTime/startTime into datetimes
		newExamples = []
		for x in examples:
			workerId, retainerRateDollars, retainerMillis, postTime, startTime = x.split("\t")
			# Remove all entries where retainerMillis != 600000 
			if int(retainerMillis) == TEN_MINUTE_RETAINER_MILLIS: 
				workerId, retainerRateDollars, retainerMillis, postTime, startTime = x.split("\t")
				startTime = startTime[0:-1]
				newX = (workerId, float(retainerRateDollars), int(retainerMillis),
								time.mktime(time.strptime(postTime, '%Y-%m-%d %H:%M:%S')), 
								time.mktime(time.strptime(startTime, '%Y-%m-%d %H:%M:%S')))
				newExamples.append(newX) 
		examples = newExamples

		# Sorts the training examples in place by startTime    
		examples.sort(key=lambda x: x[4]) 

		# Put all the delays into one array
		delays = [x[4] - x[3]  for x in examples]
		delays.sort()

		# Get rid of the delays that are more than 15 minutes
		delays = [d for d in delays if d < (15 * 60)]
		return delays 
	

# Default simulator runtime is 24 hours = 24 * 3600 sec/hour
def run_simulator(delays, num_workers_desired=5, duration_seconds=86400):
	# Amount of time we're in state x, where states = number of people 
	# online during any one second 
	histogram_dict = {}  

	num_workers_online = 0
	delayed_workers_queue = []   		# List of (task_post_time, delay_time) tuples 
	currently_working_workers = []  # List of start_time ints

	for second_step in range(duration_seconds):
		# Post tasks with a certain policy, and have workers respond with a delay	
		if num_workers_online < num_workers_desired:
			random_delay = random.choice(delays) 
			#print "random delay: ", random_delay
			delayed_workers_queue.append((second_step, random_delay))  # (task_post_time in sec, delay_time in sec)
		
		# Have all the delayed workers start working if it's their time.
		# Remove them from the delayed workers queue, and add them to the currently working list
		delayed_wkrs_to_remove = []
		for delayed_worker in delayed_workers_queue:
			post_time, delay = delayed_worker
			if second_step >= (post_time + delay):
				delayed_wkrs_to_remove.append(delayed_worker)
				currently_working_workers.append(second_step)
				num_workers_online += 1
		delayed_workers_queue = [wkr for wkr in delayed_workers_queue if wkr not in delayed_wkrs_to_remove]

		# Determine if any of the currently working workers have finished their retainer
		wkrs_to_remove = []
		for worker in currently_working_workers:
			start_time = worker
			if second_step >= (start_time + TEN_MINUTE_RETAINER_SECONDS):
				wkrs_to_remove.append(worker)
				num_workers_online -= 1
		currently_working_workers = [wkr for wkr in currently_working_workers if wkr not in wkrs_to_remove]

		# Update the histogram
		if num_workers_online in histogram_dict:
			histogram_dict[num_workers_online] += 1
		else:
			histogram_dict[num_workers_online] = 1

		#print "Workers currently online: ", num_workers_online

def main():
	delays = get_delays()
	run_simulator(delays, num_workers_desired=10)
	

if __name__ == "__main__": main()

