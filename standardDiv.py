import csv
import sys
import numpy as np
fileNameArray= []
for i in range(1,len(sys.argv)):
	if(sys.argv[i] not in fileNameArray):
		fileNameArray.append(sys.argv[i]);
for i in range(0,len(fileNameArray)):
	filename = fileNameArray[i]
	with open(filename, 'rU') as p:
		 my_list = [list(map(float,rec)) for rec in csv.reader(p, delimiter=',')]

	a = np.array(my_list)
	print fileNameArray[i] +" Standard Deviation: " + str(np.std(a))