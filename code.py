from __future__ import print_function
import csv
import sys
import re

import printt
printer=printt.printt()

import parse
parser = parse.parse()


import read_Files
reader = read_Files.read_Files()


import join
joiner = join.ClassJoin()

#---------------------Main----------------

def main():
	#print ("main DONE")
	allTables = {} #initializing tables map dict
	reader.getMetadata("metadata.txt",allTables) #extracts info from metadata.txt
	myQuery = parser.removeSpaces(str(sys.argv[1])) #get myQuery
	if myQuery[-1] == ';': #remove last semi colon, if there
		myQuery = myQuery[:-1]
	sqlIdentifiers = ["select","from","where","distinct"]
	for k in sqlIdentifiers:
		a = re.escape(k) #remove everything except numbers and alphabets
		b = re.IGNORECASE #case-insensitive matching
		c = re.compile(a,b) #make it a search object, to be used further in various functions
		myQuery = c.sub(k,myQuery) #------remove--------

	parser.sqlParse_myQuery(myQuery,allTables)



def rem_via_constants(resultant_data,const_conditions,schema,dict,tableNames):
	#print ("rem_via_constants")
	#print ("---")
	#print (const_conditions)
	#print ("---")
	new = []
	for data in resultant_data:
		#print (data)
		s = evalSQL(data,const_conditions,dict,schema,tableNames)
		#print ("===")
		#print (s)
		#print ("===")
		if len(s):
			#print ("len me ghusa")
			if eval(s):
				#print("eval me ghusa")
				new.append(data)
	resultant_data = new
	return (resultant_data,schema)

def evalSQL(data,const_conditions,dict,schema,tableNames):
	#print ("evalSQL DONE")
	parser = parse()
	const_conditions = parser.removeSpaces(re.sub('=',' = ',const_conditions))
	const_conditions = parser.removeSpaces(re.sub('<',' < ',const_conditions))
	const_conditions = parser.removeSpaces(re.sub('>',' > ',const_conditions))
	const_conditions = const_conditions.split(" ")
	#print ("-----evalSQL-----")
	#print (data)
	#print ("--------eval-----")
	string = ""
	relational = ['and','or']
	if const_conditions[0].lower() in relational:
	#print ("and or or me ghusa")
		const_conditions.pop(0)
	lhs = True
	
	for i in const_conditions:
		#print("---------"+i)

		if i == "=":
			string += i*2
		elif i == ">" or i == "<":
		#print ("more less")
			string += i
		#print (string)
		elif i.lower() == 'and' or i.lower() == 'or':
			string += ' ' + i.lower() + ' '
		#print ("after and"+string)
		elif i and lhs:
		#print (i+"ke liye elif ghusa")
			lhs = False
			if i.split('.')[0] not in dict.keys() and ('.' in i):
				sys.exit("No table found by name" + i.split('.')[0])
			else:
				try:
					i = reader.addTable([i],tableNames,dict)
					string += data[schema.index(i[0])]
				except:
					string=""
		else:
			#print ("else me")
			lhs = True
			string += i
			#print (string)
	return string


				


if __name__ == "__main__":
	main()
