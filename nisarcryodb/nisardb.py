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


class nisarcryodb():
    '''
    Abstract class to define parser for NISAR HDF products.
    '''

#    __metaclass__ = ABCMeta

    def __init__(self):
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
 

 