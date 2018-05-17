from __future__ import print_function
import csv
import sys
import re

import read_Files
reader = read_Files.read_Files()




class ClassJoin:
	def __init__(self):
		pass

	def tablesJoin(self,resultant_data,table_data,table1,table2,schema,r_att,t_att,dict):
		#print ("tablesJoin")
		if r_att and t_att:
			h = {}
			new = []
			i = schema.index(table1+"."+r_att)
		
			for idx,row in enumerate(resultant_data):
				h[row[i]] = idx
			
			i = dict[table2].index(table2+"."+t_att)

			for row in table_data:
				if h.has_key(row[i]):
					new.append(resultant_data[h[row[i]]] + row)
			resultant_data = new
			
		else:
			new = []
		#print("-")
		#print (resultant_data)
		#print ("--")
		#print (table_data)
			for r in resultant_data:
				for t in table_data:
					new.append(r+t)
			resultant_data = new
		#print ("---")
			#print (resultant_data)
		schema += dict[table2]
		#print ("----")
		#print(schema)

		return (resultant_data,schema)


	def joinConditions(self,dict,join_conditions):
		#print ("joinConditions")
		Jc = {}
		if join_conditions:
			for j in join_conditions:
				parser = parse()
				c = parser.sqlParse_condition(parser.removeSpaces(j))
				Jc[(c[0],c[1])] = (c[2],c[3])
				Jc[(c[1],c[0])] = (c[3],c[2])
		return Jc


	def join(self,dict,tableNames,const_conditions,join_conditions):
		#print ("join")
		database = {}
		visited = {}
		for t in tableNames:
			visited[t] = False
			database[t] = []
			reader.readFile(t+".csv", database[t])

		Jc = joiner.joinConditions(dict,join_conditions)
		remove_attribs = []
		i = 1
		for t in tableNames:
			if i == 1:
				resultant_data = database[t]
				visited[t] = True
				schema = dict[t]
				i = 0
			else:
				for key,value in visited.items():
					if visited[key]:
						try:
							join_attribs = Jc[(t,key)]
						except:
							join_attribs = None
						if join_attribs:
							#print ("if wala")
							remove_attribs.append(t+'.'+join_attribs[0])
							resultant_data,schema = joiner.tablesJoin(resultant_data,database[t],key,t,schema,join_attribs[1],join_attribs[0],dict)
						else:
							#print ("else wala")
							resultant_data,schema = joiner.tablesJoin(resultant_data,database[t],key,t,schema,None,None,dict)
							#print ("---")
							#print (resultant_data)
							#print ("---")
							#print (schema)

		if const_conditions:
			#print ("yaha bhis")
			#print ("const_conditions"+const_conditions)
			if "=" in const_conditions or ">" in const_conditions or "<" in const_conditions:
				#print ("yaha bhi bhi")
				if len(const_conditions):
					resultant_data,schema = rem_via_constants(resultant_data,const_conditions,schema,dict,tableNames)
					#print ("===")
					#print (resultant_data)
					#print ("===")
					#print (schema)


		return resultant_data,schema,remove_attribs

joiner = ClassJoin()



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