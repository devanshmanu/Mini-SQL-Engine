from __future__ import print_function
import csv
import sys
import re




class read_Files:
	def __init__(self):
		pass

	def getMetadata(self,myFile,dict):
		#print ("getMetadata DONE")
		with open(myFile,'rb') as f:
			flag = 0
			for metadata_line in f:
				if metadata_line.strip() == "<begin_table>":
					flag = 1
					continue
				if flag == 1:
					tableName = metadata_line.strip() #extracts the table name from the metadata i.e. metadata_line next to <begin table>
					#print (tableName)
					dict[tableName] = [];#puts tablename into list
					flag = 0
					continue
				if not metadata_line.strip() == '<end_table>':
					dict[tableName].append(tableName+"."+metadata_line.strip())#until <endtable> reached, keep adding the columns metadata to list

	def readFile(self,tName,fileData): #reading from CSV File
		#print ("readFile DONE")
		with open(tName,'rb') as f:
			reader = csv.reader(f)
			for row in reader:
				fileData.append(row)

	def addTable(self,attributes,tableNames,dict): #changing A to table1.A for no ambiguity
		#print ("addTable DONE")
		for a in attributes:
			if "." not in a:
				found = 0
				for key in tableNames:
					for v in dict[key]:
						if a == v.split('.')[1]:
							attributes[attributes.index(a)] = v
							found = 1
							break
					if found:
						break
		return attributes