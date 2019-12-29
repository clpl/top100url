from xeger import Xeger

file = open("test.txt",'w')
_x = Xeger()
for i in range(200000000):
	testStr = _x.xeger(r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
	print(testStr,file = file)
