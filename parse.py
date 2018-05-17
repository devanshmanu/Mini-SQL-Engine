from __future__ import print_function
import csv
import sys

import re

import read_Files
reader = read_Files.read_Files()

import join
joiner = join.ClassJoin()

import printt
printer=printt.printt()



def progRun(dict,tableNames,attributes,conditions,d,aggr_func):	
	#print ("progRun")
	conditions = re.sub(' ?= ?','=',conditions)
	parser = parse()
	conditions = [parser.removeSpaces(c) for c in conditions.split(" ")]
	#conditions is now a lst
	if len(conditions[0]):
		resultant_data = []
		join_conditions = []
		const_conditions = ""

		for c in conditions:
			x = c.split("=")
			try:
				RHS = parser.removeSpaces(x[1])
			except:
				RHS=""
			if '.' not in RHS:
				#print ("yaha")
				const_conditions += " " + c + " " #just gives the conditional arguments A<0``
				#print (const_conditions)


		const_conditions = parser.removeSpaces(const_conditions)
		#print("const are"+const_conditions)

		for c in conditions:
			x = c.split('=')
			try:
				RHS = parser.removeSpaces(x[1])
			except:
				RHS =""
			if '.' in RHS:
				join_conditions.append(c)
		resultant_data,schema,remove_attribs = joiner.join(dict,tableNames,const_conditions,join_conditions)
		printer.sqlPrint(resultant_data,attributes,schema,d,aggr_func,remove_attribs)
	
	elif len(tableNames) > 1:
		resultant_data,schema,remove_attribs = joiner.join(dict,tableNames,None,None)
		printer.sqlPrint(resultant_data,attributes,schema,d,aggr_func,None)

	else:
		resultant_data = []
		schema = dict[tableNames[0]]
		reader.readFile(tableNames[0]+".csv", resultant_data)
		printer.sqlPrint(resultant_data,attributes,schema,d,aggr_func,None)



def verifyTables(tableNames,dict):
	#print ("verifyTables DONE")
	for t in tableNames:
		try:
			d = dict[t]
		except:
			sys.exit(str(tableNames)+": Not found") #checking if the table name extracted is correct



def verifyAttri(attributes,tablenames,dict): #checking if the attribute is present in table or not
	#print ("verifyAttri DONE")
	try:
		if attributes[0] == "*":
			attributes = dict[tablenames[0]]
		else:
			for a in attributes:
				present = False
				for key,value in dict.items():
					if a in value:
						if present:
							sys.exit("Ambigious case for attibute "+a)
						present = True
						break
				if not present:
					sys.exit("Attribute "+ a + " not present")
	except:
		sys.exit("Attribute not found")
	return attributes


class parse:
	def __init__(self):
		pass

	def removeSpaces(self,q):
		return (re.sub(' +',' ',q)).strip()

	def sqlParse_myQuery(self,myQuery,dict):
		#print ("sqlParse_myQuery DONE")
		myQuery = parser.removeSpaces(myQuery)
		#print(myQuery)
		d = tuple()
		if "from" in myQuery:
			from_split = myQuery.split('from') #from_spilt[0]="select A,B" from_spilt[1]=" table1 where ---"
		else:
			sys.exit("FROM clause not found") #from not found

		select_and_attributes_part = from_split[0].strip()

		if "select" in select_and_attributes_part:
			select_split = select_and_attributes_part.strip().split('select')[1] #stores what all to select, i.e list of all attributes
		else:
			sys.exit("No select attributes present")


		select_split = parser.removeSpaces(select_split)
		if "distinct" in select_split:
			attributes = parser.removeSpaces(select_split.strip().split('distinct')[1]) #just so that distinct doesnt come in the attibutes when splited
			distinct = True #setting distinct flag at TRUE
			if len(parser.removeSpaces(select_split.strip().split('distinct')[0])): #handling case like "select distinct from"
				sys.exit("Incorrect myQuery format")
		else:
			attributes = select_split #add all attriutes because distinct isnt in myQuery
			distinct = False #setting distinct flag as FALSE
		attributes = parser.removeSpaces(attributes)
		attributes = [parser.removeSpaces(x) for x in attributes.split(',')] #just cleaning the attributes

		where_split = parser.removeSpaces(from_split[1]).split('where')  #has 'table1,table2' 'condition'. where_split[1]onwards condition
		tableNames = [parser.removeSpaces(x) for x in parser.removeSpaces(where_split[0]).split(',')] #get tables names
		verifyTables(tableNames,dict) #checking if table extracted is in dict

		if distinct:
			for a in attributes:
				if '(' in a or ')' in a:
					d_att = a.split('(')[1][:-1]
					attributes[attributes.index(a)] = d_att
					break
			aggr_func = ""
			x = reader.addTable([d_att],tableNames,dict)
			d = (x[0],True)

		temp = []
		aggr_func = ""
		for a in attributes:
			if '(' in a or ')' in a:
				aggr_func = a
			else:
				temp.append(a)
		attributes = temp

		if aggr_func:
			if '.' not in aggr_func:
				attributes = [parser.removeSpaces(aggr_func).split('(')[1][:-1]]
				attributes = reader.addTable(attributes,tableNames,dict)
				aggr_func = parser.removeSpaces(aggr_func).split('(')[0] + '(' + attributes[0] + ')'

		attributes = reader.addTable(attributes,tableNames,dict)
		attributes = verifyAttri(attributes,tableNames,dict)

		
		if len(where_split) > 1:
			conditions = parser.removeSpaces(where_split[1]) #as where_split had only left and right of where. 
		else:
			conditions = ""

		progRun(dict,tableNames,attributes,conditions,d,aggr_func)

	def sqlParse_condition(self,condition):
		#print ("sqlParse_condition DONE")
		x = condition.split("=")
		L = []
		L.append(parser.removeSpaces(x[0]).split('.')[0])
		L.append(parser.removeSpaces(x[1]).split('.')[0])
		L.append(parser.removeSpaces(x[0]).split('.')[1])
		L.append(parser.removeSpaces(x[1]).split('.')[1])
		return tuple(L)



parser=parse()