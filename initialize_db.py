# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 21:23:56 2025

@author: aysha

A script to create the initial database.
"""

import pandas as pd
import random
import os
from sqlalchemy import create_engine  # For converting csv to table
import psycopg2  # For connecting to db...easier than sqlalchemy but redundant

##  BOOK DATA  ##

# Import csv for book data
df = pd.read_csv('data/books.csv',on_bad_lines='skip')

# Remove columns not in our schema
new_df = df.drop(['bookID','isbn13','text_reviews_count'],axis=1)

# List of values to randomly assign
locations = ['001', '002', '003', '004']

# Randomly assign values from the list to a new column 'location'
new_df['branch'] = [random.choice(locations) for _ in range(len(df))]
# Give default status of 'available' to all books 
new_df['status'] = 'available'

# Now book data is ready! 


##  POSTGRESQL CONNECTION  ##

# Define variables for PostgreSQL connection
host = 'localhost'
dbname = 'postgres'
user = 'postgres'
password = '1234'
port = '5432'
database = 'postgres'

#DATABASE_URL = os.getenv("postgresql://csc6710:VTUTpwJbXh8AgVmu9FObBduOoSYBMlRY@dpg-d00uv1qdbo4c73dmj7qg-a.virginia-postgres.render.com/library_qzjp")
DATABASE_URL = f'postgresql://{user}:{password}@{host}:{port}/{database}'

# Create SQLAlchemy engine 
engine = create_engine(DATABASE_URL)
    #f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Create book table
new_df.to_sql('book', engine, if_exists='replace', index=False)

# Create connection to server
conn = psycopg2.connect(DATABASE_URL)#host='localhost', dbname='postgres',
                        #user='postgres', password='1234', port=5432)

# Create cursor
cur = conn.cursor()

# Assign primary key to books 
cur.execute("""ALTER TABLE book ADD PRIMARY KEY (isbn)""");

# Create libraryBranch
cur.execute("""DROP TABLE IF EXISTS libraryBranch CASCADE;""")
cur.execute("""CREATE TABLE IF NOT EXISTS libraryBranch(
branchID VARCHAR(50) PRIMARY KEY,
location VARCHAR(50));
""")

# Create staff
cur.execute("""DROP TABLE IF EXISTS staff;""")
cur.execute("""CREATE TABLE IF NOT EXISTS staff(
staffID INT PRIMARY KEY,
branch VARCHAR(50) REFERENCES libraryBranch(branchID),
firstName VARCHAR(50),
lastName VARCHAR(50),
phone CHAR(10),
email VARCHAR(50) UNIQUE,
street VARCHAR(50),
city VARCHAR(50),
state CHAR(2),
zipcode CHAR(5));
""")

# Create patron
cur.execute("""DROP TABLE IF EXISTS patron;""")
cur.execute("""CREATE TABLE IF NOT EXISTS patron(
libraryCardID INT PRIMARY KEY,
PIN CHAR(10),
cardExpire DATE,
firstName VARCHAR(50),
lastName VARCHAR(50),
phone CHAR(10),
email VARCHAR(50) UNIQUE,
street VARCHAR(50),
city VARCHAR(50),
state CHAR(2),
zipcode CHAR(5),
bookHistory VARCHAR(50) [],
fees NUMERIC);
""")


# Insert sample library branches (Neopets names)
cur.execute("""INSERT INTO libraryBranch(branchID,location) VALUES
('001','Meridell'),
('002','Brightvale'),
('003','Faerieland'),
('004','Moltara');
""")


# Insert sample staff
cur.execute("""INSERT INTO staff(staffID,branch,firstName,lastName,phone,email,street,city,state,zipcode) VALUES
(1, '003','Seshatia','Fae','4046664242','fae_librarian@neopets.com','123 E. Fyora Street','Fyoraville','FL','30003');
""")

# Commit
conn.commit()

# Close
cur.close()
conn.close()
