# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 21:23:56 2025

@author: aysha

A script to create the initial database.
""" 

import pandas as pd
import random
from sqlalchemy import create_engine, text

##  BOOK DATA  ##

# Import csv for book data
df = pd.read_csv('data/books.csv', on_bad_lines='skip')

# Remove columns not in our schema
new_df = df.drop(['bookID', 'isbn13', 'text_reviews_count'], axis=1)

# List of values to randomly assign
locations = ['001', '002', '003', '004']

# Randomly assign values from the list to a new column 'location'
new_df['branch'] = [random.choice(locations) for _ in range(len(df))]
# Give default status of 'available' to all books 
new_df['status'] = 'available'

# Now book data is ready! 
##############################

##  SQLALCHEMY CONNECTION  ##

# Define database URL
DATABASE_URL = "postgresql://csc6710:y6HKYcwrYdH3XBfr6m6Wjmkzlh1lG0Kr@dpg-d017lgvgi27c73a2k0i0-a.virginia-postgres.render.com/librarydb_ga0x"

# Create SQLAlchemy engine 
engine = create_engine(DATABASE_URL)

# To handle dependencies before creating book, drop table with CASCADE 
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS book CASCADE;"))
    conn.commit()

# Create book table
new_df.to_sql('book', engine, if_exists='replace', index=False)

##############################

##  CREATE ENTITY TABLES  ##

# Create all tables using SQLAlchemy
with engine.connect() as conn:
    # Assign primary key to books 
    conn.execute(text("ALTER TABLE book ADD PRIMARY KEY (isbn)"))
    
    # Create libraryBranch
    conn.execute(text("DROP TABLE IF EXISTS libraryBranch CASCADE"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS libraryBranch(
        branchID VARCHAR(50) PRIMARY KEY,
        location VARCHAR(50),
        pass VARCHAR(50))"""))
    
    # Create staff
    conn.execute(text("DROP TABLE IF EXISTS staff CASCADE"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS staff(
        staffID INT PRIMARY KEY,
        staffPIN VARCHAR(50)
        branch VARCHAR(50) REFERENCES libraryBranch(branchID),
        firstName VARCHAR(50),
        lastName VARCHAR(50),
        phone CHAR(10),
        email VARCHAR(50) UNIQUE,
        street VARCHAR(50),
        city VARCHAR(50),
        state CHAR(2),
        zipcode CHAR(5))"""))
    
    # Create patron
    conn.execute(text("DROP TABLE IF EXISTS patron CASCADE"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS patron(
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
        fees NUMERIC)"""))
    
    ##############################
    
    ##  CREATE RELATION TABLES  ##
    # libraryBranch hires staff
    conn.execute(text("DROP TABLE IF EXISTS hiresStaff"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS hiresStaff(
        branchID VARCHAR(50) REFERENCES libraryBranch(branchID),
        staffID INT REFERENCES staff(staffID))"""))
    
    # patron borrows book
    conn.execute(text("DROP TABLE IF EXISTS borrows"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS borrows(
        libraryCardID INT REFERENCES patron(libraryCardID),
        isbn VARCHAR(10) REFERENCES book(isbn),
        holdStatus INT,
        renewals INT,
        dueDate DATE,
        PRIMARY KEY (libraryCardID, isbn))"""))
    
    # patron reviews book
    conn.execute(text("DROP TABLE IF EXISTS reviews"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS reviews(
        libraryCardID INT REFERENCES patron(libraryCardID),
        isbn VARCHAR(10) REFERENCES book(isbn),
        rating NUMERIC,
        text TEXT,
        PRIMARY KEY (libraryCardID, isbn))"""))
    
    # branch stocks book
    conn.execute(text("DROP TABLE IF EXISTS stocks"))
    conn.execute(text("""CREATE TABLE IF NOT EXISTS stocks(
        isbn VARCHAR(10) REFERENCES book(isbn),
        branch VARCHAR(50) REFERENCES libraryBranch(branchID),
        PRIMARY KEY (isbn, branch))"""))
    
    ##############################
    
    ##  INITIAL SAMPLE DATA  ##
    
    # Insert sample library branches (Neopets names)
    conn.execute(text("""INSERT INTO libraryBranch(branchID,location) VALUES
        ('001','Meridell','mirth'),
        ('002','Brightvale','knowledge'),
        ('003','Faerieland','magic'),
        ('004','Moltara','lava)"""))
    
    # Insert sample staff
    conn.execute(text("""INSERT INTO staff(
        staffID,branch,firstName,lastName,phone,email,street,city,state,zipcode) 
        VALUES(1, '003','Seshatia','Fae','4046664242','fae_librarian@neopets.com','123 E. Fyora Street','Fyoraville','FL','30003')"""))
    
    conn.commit()