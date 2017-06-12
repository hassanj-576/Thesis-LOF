import random
import time
import sys

print "Hello World"
limit =0
limitBase=0
limitIncrement=2
anomoallyIncrement=25
temp=""
dimensions=int(sys.argv[4])
random.seed( time.time() )
f = open(sys.argv[1], 'w')
for loopPass in xrange(0,int(sys.argv[2])):
	for x in xrange(0,int(sys.argv[3])):
		limit=limitBase+random.uniform(0,3) 
		temp=""
		for y in xrange(0,dimensions):
			if x%33==0:
				tempLimit=limit+random.uniform(0,anomoallyIncrement)
				number1 =random.uniform(0,tempLimit)
				temp=temp+str(number1)+","
			else:
				temp=temp+str(random.uniform(0,limit))+","

		temp = temp[:-1]
		f.write(temp+"\n") 
	limitBase=limitBase+limitIncrement


f.close()
# for x in xrange(1,1000):
# 	limit2=limit+random.uniform(1,3) 
# 	if x%33==0:
# 		tempLimit=limit+random.uniform(0,15)
# 		number1 =random.uniform(0,tempLimit)
# 		number2=random.uniform(0,tempLimit)
# 		f.write(str(number1)+","+str(number2)+"\n") 
# 	else:
# 		number1=random.uniform(2,limit2)
# 		number2=random.uniform(0,limit2)
# 		f.write(str(number1)+","+str(number2)+"\n") 
# for x in xrange(1,1000):
# 	limit3=limit2+random.uniform(1,3) 
# 	if x%33==0:
# 		tempLimit=limit+random.uniform(0,15)
# 		number1 =random.uniform(0,tempLimit)
# 		number2=random.uniform(0,tempLimit)
# 		f.write(str(number1)+","+str(number2)+"\n") 
# 	else:
# 		number1=random.uniform(4,limit3)
# 		number2=random.uniform(0,limit3)
# 		f.write(str(number1)+","+str(number2)+"\n") 

# for x in xrange(1,1000):
# 	limit4=limit3+random.uniform(1,3) 
# 	if x%33==0:
# 		tempLimit=limit+random.uniform(0,15)
# 		number1 =random.uniform(0,tempLimit)
# 		number2=random.uniform(0,tempLimit)
# 		f.write(str(number1)+","+str(number2)+"\n") 
# 	else:
# 		number1=random.uniform(6,limit4)
# 		number2=random.uniform(0,limit4)
# 		f.write(str(number1)+","+str(number2)+"\n") 

# for x in xrange(1,1000):
# 	limit5=limit4+random.uniform(1,3) 
# 	if x%33==0:
# 		tempLimit=limit+random.uniform(0,15)
# 		number1 =random.uniform(0,tempLimit)
# 		number2=random.uniform(0,tempLimit)
# 		f.write(str(number1)+","+str(number2)+"\n") 
# 	else:
# 		number1=random.uniform(8,limit5)
# 		number2=random.uniform(0,limit5)
# 		f.write(str(number1)+","+str(number2)+"\n") 


