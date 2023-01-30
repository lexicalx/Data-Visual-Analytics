"""
cse6242 s23
Q5.py - utilities to supply data to the templates.

This file contains a pair of functions for retrieving and manipulating data
that will be supplied to the template for generating the table. """
import csv

def username():
    return 'tjordan60'

def data_wrangling():
    with open('data/movies.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        table = list()
        # Feel free to add any additional variables
        ...
        
        # Read in the header
        for header in reader:
            break
        
        # Read in each row
        c = 0
        for row in reader:
            if c == 100:
                break
            c += 1
            table.append(row)
            # Only read first 100 data rows - [2 points] Q5.a
            ...

        
        # Order table by the last column - [3 points] Q5.b
        ...
        s_table = sorted(table,key= lambda x: float(x[2]), reverse=True)
    
    return header, s_table

