#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 08:31:25 2024

@author: ian
"""
import numpy as np
import calendar
from datetime import timedelta, datetime
import os
import pyproj
from scipy.stats import linregress
import psycopg2
import warnings
import getpass
import configparser
from psycopg2.extensions import AsIs
from psycopg2 import sql
import pandas as pd

class nisarcryodb():
    '''
    Abstract class to define parser for NISAR HDF products. Based on examples provided by Brandi and Catalina
    '''

#    __metaclass__ = ABCMeta

    def __init__(self, configFile='calvaldb_config.ini'):
        '''
        Class for retrieving cryo data from NISAR cal/val database

        Parameters
        ----------
      
        **keywords : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        self.initDB(configFile)

    def initDB(self, configFile):
        ''' Establish a connection to the database based on Brandi's Notebook
        
        Parameters
        ----------
        configFile : str
            Cal/val configuration file
        
        '''
        if configFile is not None:
            self.configFile = configFile
            
        file = os.path.expanduser(self.configFile)
    
        # Check if the file exists
        if not os.path.exists(file):
            raise FileNotFoundError(f"Configuration file not found: {file}")
    
        # Parse the configuration file
        config = configparser.ConfigParser()
        try:
            config.read(file)
        except Exception as e:
            raise Exception(f"Configuration file error: {e}")
        # Get connection properties
        conn_properties = config['DEFAULT']
        dbname = conn_properties.get('dbname')
        host = conn_properties.get('host')
        port = conn_properties.get('port')
        if not dbname or not host or not port:
            raise Exception("Missing required properties")
            
        # Get username and password    
        user = input('User name: ')
        password = getpass.getpass('Password: ')
        #
        self.connection = psycopg2.connect(dbname=dbname, host=host,
                                           port=port, user=user, password=password)  
        self.cursor = self.connection.cursor()
        
    def listSchema(self):
    # List all schema in the database
        query = "SELECT schema_name FROM information_schema.schemata;"
        self.cursor.execute(query)
        schemas = self.cursor.fetchall()
        schemas = [k[0] for k in schemas]
        print(*schemas, sep='\n')

    def listSchemaTableNames(self, schemaName):
    # 
        query = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %(schemaName)s;"
        self.cursor.execute(query, {'schemaName': schemaName})
        tables = self.cursor.fetchall()
        tables = [k[0] for k in tables]
        print(*tables, sep='\n')
        return tables

    def listTableColumns(self, schemaName, tableName, returnType=False, quiet=False):
        query = "SELECT column_name, data_type FROM information_schema.columns "
        query += "WHERE table_schema = %(schema_Name)s AND table_name = %(table_Name)s;"
        self.cursor.execute(query, {'schema_Name': schemaName, 'table_Name': tableName})
        columns = self.cursor.fetchall()
        columnNames = [k[0] for k in columns]
        if not quiet:
            print(', '.join(columnNames))
        if returnType:
            types = [k[1] for k in columns]
            return columnNames, types
        return columnNames

    def getColumn(self, schemaName, tableName, columnName):
        query = sql.SQL("SELECT DISTINCT {} FROM " + f"{schemaName}.{tableName};")
        self.cursor.execute(query.format(sql.Identifier(columnName)))
        uniqueVals = self.cursor.fetchall()
        uniqueVals = [k[0] for k in uniqueVals]
        return uniqueVals

    def getStations(self, schemaName='landice', tableName='gps_station'):
        query = f"SELECT * FROM {schemaName}.{tableName};"
        self.cursor.execute(query, {})
        return pd.DataFrame(self.cursor.fetchall(),
                            columns=self.listTableColumns(schemaName, tableName))
        
    def getStationDateRangeData(self, stationName, d1, d2, schemaName, tableName):
        stationID = self.stationNameToID(stationName)
        query = f"SELECT * FROM {schemaName}.{tableName} " + "WHERE decimal_year BETWEEN %(val1)s AND %(val2)s AND station_id = %(station_id)s;"
        self.cursor.execute(query, {'val1': d1, 'val2': d2, 'station_id': stationID})
        return pd.DataFrame(self.cursor.fetchall(),
                            columns=self.listTableColumns(schemaName, tableName))

    def stationNameToID(self, stationName, schemaName='landice', tableName='gps_station'):  
        query = f"SELECT * FROM {schemaName}.{tableName} " + "WHERE station_name = %(station_name)s;"
        self.cursor.execute(query, {'station_name': stationName})
        values = self.cursor.fetchall()[0]
        keys = self.listTableColumns(schemaName, tableName, quiet=True)
        lookup = dict(zip(keys, values))
        return lookup['station_id']
    
    def closeDB(self):
        # Close the cursor and connection
        self.cursor.close()
        self.connection.close()
