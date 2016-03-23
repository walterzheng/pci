# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 23:42:47 2016

@author: walterzheng
"""

from random import random, randint
import math

def WinePrice(rating, age):
    peak_age = rating - 50
     
    price = rating / 2
    if age > peak_age:
        price = price * (5 - (age - peak_age))
    else:
        price = price * (5 * (age+1) / peak_age)
        
    if price < 0:
        price = 0
    return price