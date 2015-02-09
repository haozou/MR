import random

filer = open('undir', 'r')
filew = open('sample_300w', 'a+')
dic = {}
lines = filer.readlines()
for i in range(0, 3000000):
	a = random.randint(1, 21776094)
	if dic.has_key(a):
		pass
	else:
		dic[a] = True
		filew.write(lines[a])

filer.close()
filew.close()
