from xeger import Xeger
import random

file = open("test.txt",'w')

_x = Xeger(seed=0)
for i in range(20000000):
	if random.randint(0,50) == 1:
		_x = Xeger(seed=1)
	testStr = _x.xeger(r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
	print(testStr,file = file)
