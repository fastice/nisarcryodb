#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 08:31:25 2024

@author: ian
"""

import os
import psycopg2
import getpass
import configparser
from psycopg2 import sql
import pandas as pd

class nisarcryodb():
    '''
    Abstract class to define parser for NISAR HDF products. Based on examples 
    provided by Brandi and Catalina.
    '''

#    __metaclass__ = ABCMeta

    def __init__(self, configFile='calvaldb_config.ini'):
        '''
        Class for retrieving cryo data from NISAR cal/val database

        Parameters
        ----------
        configFile : str, optional
            Data base cconfig file. The default is 'calvaldb_config.ini'.

        Returns
        -------
        None.

        '''
        
        self._initDB(configFile)

    def _initDB(self, configFile):
        ''' 
        Establish a connection to the database based on Brandi's Notebook
        
        Parameters
        ----------
        configFile : str, optional
            Data base cconfig file. The default is 'calvaldb_config.ini'.

        Returns
        -------
        None.

        '''
        if configFile is not None:
            self.configFile = configFile
            
        file = os.path.expanduser(self.configFile)
        #
        # Check if the file exists
        if not os.path.exists(file):
            raise FileNotFoundError(f"Configuration file not found: {file}")
        #
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
        #
        if not dbname or not host or not port:
            raise Exception("Missing required properties")
        #    
        # Get username and password    
        user = input('User name: ')
        password = getpass.getpass('Password: ')
        #
        self.connection = psycopg2.connect(dbname=dbname,
                                           host=host,
                                           port=port, 
                                           user=user,
                                           password=password)  
        self.cursor = self.connection.cursor()
        
    def listSchema(self, quiet=False):
        '''
        
        List all schema in the database
        Parameters
        ----------
        quiet : bool, optional
            Set to true to avoid printing result. The default is False.

        Returns
        -------
        schemas, list.
            List of the schemas

        '''
        query = "SELECT schema_name FROM information_schema.schemata;"
        self.cursor.execute(query)
        schemas = self.cursor.fetchall()
        schemas = [k[0] for k in schemas]
        if not quiet:
            print(*schemas, sep='\n')
        return schemas

    def listSchemaTableNames(self, schemaName, quiet=False):
        '''
        Parameters
        ----------
        schemaName : TYPE
            DESCRIPTION.
        quiet : bool, optional
            Set to true to avoid printing result. The default is False.

        Returns
        -------
        tables, list.
            List of the tables
        '''
        # 
        query = "SELECT tablename FROM pg_catalog.pg_tables WHERE " \
            "schemaname = %(schema_Name)s;"
        self.cursor.execute(query, {'schema_Name': schemaName})
        tables = [k[0] for k in self.cursor.fetchall()]
        if not quiet:
            print(*tables, sep='\n')
        return tables

    def listTableColumns(self, schemaName, tableName, returnType=False, quiet=False):
        '''
        List the column nmes for a table

        Parameters
        ----------
        schemaName : str
            Name of the schema.
        tableName : str
            Name of the table.
        returnType : bool, optional
            DESCRIPTION. The default is False.
        quiet : bool, optional
            Set to true to avoid printing result. The default is False.

        Returns
        -------
        list[s]
            Either just the col names (returnType=False) or additionally a
            list with the types for each column (returnType=True).

        '''
        query = "SELECT column_name, data_type " \
            "FROM information_schema.columns WHERE " \
            "table_schema = %(schema_Name)s AND table_name = %(table_Name)s;"
        # do query
        self.cursor.execute(query, {'schema_Name': schemaName,
                                    'table_Name': tableName})
        columns = self.cursor.fetchall()
        #
        columnNames = [k[0] for k in columns]
        if not quiet:
            print(', '.join(columnNames))
        if returnType:
            types = [k[1] for k in columns]
            if not quiet:
                print(', '.join(types))
            return columnNames, types
        return columnNames

    def getColumn(self, schemaName, tableName, columnName):
        '''
        Get values for a column schemaName.tableName

        Parameters
        ----------
        schemaName : str
            Name of the schema.
        tableName : str
            Name of the table.
        columnName : str
            column name

        Returns
        -------
        uniqueVals : list
            The values for the column.

        '''
        query = sql.SQL("SELECT DISTINCT {} FROM "
                        f"{schemaName}.{tableName};")
        #
        self.cursor.execute(query.format(sql.Identifier(columnName)))
        return [k[0] for k in self.cursor.fetchall()]

    def getStationsListing(self, schemaName='landice',
                               tableName='gps_station'):
        '''
        Get the station information (e.g. station_id, station_name, ref_lat...)

        Parameters
        ----------
        schemaName : str optional
            Name of schema. The default is 'landice'.
        tableName : str, optional
            Name of table with station info . The default is 'gps_station'.

        Returns
        -------
        pandas data frame
            Pandas table with the station parameters

        '''
        query = f"SELECT * FROM {schemaName}.{tableName};"
        self.cursor.execute(query, {})
        return pd.DataFrame(self.cursor.fetchall(),
                            columns=self.listTableColumns(schemaName,
                                                          tableName))
        
    def getStationDateRangeData(self, stationName, d1, d2, 
                                schemaName='landice', tableName='gps_data'):
        '''
        Return as a pandas data fram the results for stationName for the 
        inveral [d1, d2]

        Parameters
        ----------
        stationName : str
            Station name (e.g., NIT3).
        d1 : float
            Decimal date for start of window.
        d2 : float
            Decimal date for start of window.
        schemaName : str, optional
            Schema name. The default is 'landice'
        tableName : str, optional
            Name of data table. The default is 'gps_data'.

        Returns
        -------
        pandas data frame
            GPS data for the station and date range..

        '''
        stationID = self.stationNameToID(stationName)
        query = f"SELECT * FROM {schemaName}.{tableName} WHERE decimal_year " \
            "BETWEEN %(val1)s AND %(val2)s AND station_id = %(station_id)s;"
        #
        self.cursor.execute(query,
                            {'val1': d1, 'val2': d2, 'station_id': stationID})
        return pd.DataFrame(self.cursor.fetchall(),
                            columns=self.listTableColumns(schemaName,
                                                          tableName))

    def stationNameToID(self, stationName, 
                        schemaName='landice', tableName='gps_station'):  
        '''
        Return the station_id given the station_name

        Parameters
        ----------
        stationName : str
            Station name (e.g., NIT3).
        schemaName : str, optional
            Schema name. The default is 'landice'
        tableName : str, optional
            Name of data table. The default is 'gps_data'.

        Returns
        -------
        int
            Station id.

        '''
        query = f"SELECT * FROM {schemaName}.{tableName} " \
            "WHERE station_name = %(station_name)s;"
        #
        self.cursor.execute(query, {'station_name': stationName})
        # Build a dict to map name to id
        values = self.cursor.fetchall()[0]
        keys = self.listTableColumns(schemaName, tableName, quiet=True)
        lookup = dict(zip(keys, values))
        # return result for specified 
        return lookup['station_id']
    
    def close(self):
        '''
        Close the cursor and connection

        Returns
        -------
        None.

        '''
        self.cursor.close()
        self.connection.close()
