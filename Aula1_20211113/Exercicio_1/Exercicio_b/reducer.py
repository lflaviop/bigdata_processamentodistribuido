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
		dic_airports = {}
		for current, group in groupby(self, itemgetter(0)):
			total = 0 

			for item in group:
				total += item[1]
			
			dic_airports[current] = total
		
		max_count = int(max(dic_airports.values()))
		airport = list(dic_airports.keys())[list(dic_airports.values()).index(max_count)]

		self.emit(airport, max_count)

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
