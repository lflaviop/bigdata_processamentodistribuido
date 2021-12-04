#!/usr/bin/env python3

import sys

from itertools import groupby
from operator import itemgetter

SEP = "\t"

class Reducer(object):

	def __init__(self, infile=sys.stdin, separator=SEP):
		self.infile = infile
		self.sep = separator

	def emit(self, key, value):
		sys.stdout.write(f"{key}{self.sep}{value}\n")

	def reduce(self):
		for current, group in groupby(self, itemgetter(0)):
			dicio_flights = {}
			dicio_max_flight = {}

			for item in group:
				if item[1] in dicio_flights:
					dicio_flights[item[1]] += 1
				else:
					dicio_flights[item[1]] = 1
			
			max_count = int(max(dicio_flights.values()))
			flight = int(list(dicio_flights.keys())[list(dicio_flights.values()).index(max_count)])
			dicio_max_flight[flight] = max_count

			self.emit(current, dicio_max_flight)

	def __iter__(self):
		for line in self.infile:
			try:
				parts = line.split(self.sep)
				yield parts[0], float(parts[1])
			except:
				continue

if __name__ == '__main__':
	reducer = Reducer(sys.stdin)
	reducer.reduce()
