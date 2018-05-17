from __future__ import print_function
import csv
import sys
import re



class printt:
	def __init__(self):
		pass

	def sqlPrint(self,resultant_data,attributes,schema,d,aggr_func,remove_attribs):
		#print ("sqlPrint")
		if len(d):
			i = 1
			for a in attributes:
				if i == 1:
					print(a,end="")
					i = 0
				else:
					print(","+a,end="")
			print("\n")

			h = {}
			for idx,row in enumerate(resultant_data):
				try:
					if h[row[schema.index(d[0])]]:
						continue
				except:
					h[row[schema.index(d[0])]] = idx

			for key,value in h.items():
				i = 1
				for col in attributes:
					data = resultant_data[value]
					if i == 1:
						i = 0
						print(data[schema.index(col)],end="")
					else:
						print(","+data[schema.index(col)],end="")
				print("\n")

		elif aggr_func:
			parser = parse()
			type_of_func = parser.removeSpaces(parser.removeSpaces(aggr_func).split('(')[0]).lower()
			printer.printSQLAggregate(type_of_func,resultant_data,schema,parser.removeSpaces(parser.removeSpaces(aggr_func).split('(')[1][:-1]))

		else:
			i = 1
			for a in attributes:
				if remove_attribs:
					if a in remove_attribs:
						continue
				if i == 1:
					print(a,end="")
					i = 0
				else:
					print(","+a,end="")
			print("\n")
			
			for data in resultant_data:
				i = 1
				for col in attributes:
					if remove_attribs:
						if col in remove_attribs:
							continue
					if i == 1:
						print(data[schema.index(col)],end="")
						i = 0
					else:
						print(","+data[schema.index(col)],end="")
				print("\n")

	def printSQLAggregate(self,tof,res,schema,agg_att):
		#print("printSQLAggregate")
		l = []
		for r in res:
			l.append(int(r[schema.index(agg_att)]))
		if tof == "sum":
			print("Sum of "+agg_att)
			print(sum(l))
		elif tof == "avg":
			print("Avg of "+agg_att)
			print(sum(l)/len(l))
		elif tof == "max":
			print("max of "+agg_att)
			print(max(l))
		elif tof == "min":
			print("min of "+ agg_att)
			print(min(l))
		else:
			sys.exit("Unknown aggregate function")

printer = printt()

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