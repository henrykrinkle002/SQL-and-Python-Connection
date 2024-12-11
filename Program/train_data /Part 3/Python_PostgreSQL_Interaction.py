#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 23:57:00 2024

"""

import psycopg2
import pandas as pd
from decimal import Decimal

def getConn():
     with open("pw.txt", "r") as pwFile:
        pw = pwFile.read().strip();
     
     connStr = f"host='cmpstudb-01.cmp.uea.ac.uk'\
                   dbname='uph24dqu' user='uph24dqu' password='{pw}'"
     conn = psycopg2.connect(connStr)      
     return conn

def clearOutput():
     with open("output.txt", "w") as clearfile:
         clearfile.write('')
        
def writeOutput(output):
     with open("output.txt", "a") as myfile:
         myfile.write(output)
         
# clearOutput()

try:
     conn=None   
     conn=getConn()
#    # All the sql statement once run will be autocommited
     conn.autocommit=True
     cur = conn.cursor()
     cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)   

     f = open("input.txt", "r")
     clearOutput()
     for x in f:
        print(x)
        if(x[0] == 'A'):
             raw = x.split("#",1)
             raw[1] = raw[1].strip();
             data = raw[1].strip().split('#')
             try:
                cur.execute("SET SEARCH_PATH to public;");
                sql = "INSERT INTO BOOK (bno, title, author, category, price)\
                VALUES ({}, '{}', '{}', '{}',{});".format(data[0], data[1], data[2], data[3], data[4])
                writeOutput("TASK "+x[0]+"\n");
                cur.execute(sql);
                table_df = pd.read_sql_query(sql, conn);
                table_str = table_df.to_string()
                writeOutput(table_str+"\n");
             except Exception as e:
                print(e)

             try:
                cur.execute("SET SEARCH_PATH TO public;");
                sql="SELECT * FROM book;"
                # Sending the query and connection object to read_sql_query method of pandas pd. It reurns table as dataframe.
                table_df=pd.read_sql_query(sql, conn)
                # Converting dataframe to string so that it can be written to text file.
                table_str=table_df.to_string()
                writeOutput(table_str+"\n")
             except Exception as e:
                print (e)
                
        if(x[0] == 'B'):
            raw = x.split('#',1);
            raw[1] = raw[1].strip();
            data = raw[1].strip().split();
            try:
                cur.execute("SET SEARCH_PATH to public;");
                sql = "DELETE FROM book  \
                       WHERE bno = {};".format(data[0]);
                writeOutput("TASK "+x[0]+"\n");
                cur.execute(sql);
                table_df = pd.read_sql_query(sql, conn);
                table_str = table_df.to_string();
                writeOutput(table_str+"\n");
            except Exception as e:
                #writeOutput(str(e)+"\n"); 
                print(e);          
                
            try:
                cur.execute("SET SEARCH_PATH TO public;");
                sql="SELECT * FROM book;"
                # Sending the query and connection object to read_sql_query method of pandas pd. It reurns table as dataframe.
                table_df=pd.read_sql_query(sql, conn)
                # Converting dataframe to string so that it can be written to text file.
                table_str=table_df.to_string()
                writeOutput(table_str+"\n")
            except Exception as e:
                print()
            
        if(x[0] == 'C'):
              raw = x.split('#', 1);
              raw[1] = raw[1].strip();
              data = raw[1].strip().split('#');
              try:
                cur.execute("SET SEARCH_PATH to public;");
                sql = "INSERT INTO CUSTOMER (cno, name, address)\
                       VALUES({},'{}','{}')".format(data[0], data[1], data[2]);
                writeOutput("TASK "+x[0]+"\n");
                cur.execute(sql);
                table_df = pd.read_sql_query(sql, conn);
                table_str = table_df.to_string();
                writeOutput(table_str+"\n");
              except Exception as e:
                print(str(e)+"\n");

              try:
                  cur.execute("SET SEARCH_PATH TO public;");
                  sql="SELECT * FROM customer;"
                  # Sending the query and connection object to read_sql_query method of pandas pd. It reurns table as dataframe.
                  table_df=pd.read_sql_query(sql, conn)
                  # Converting dataframe to string so that it can be written to text file.
                  table_str=table_df.to_string()
                  writeOutput(table_str+"\n")
              except Exception as e:
                  print()
 
        if(x[0] == 'D'):
            raw = x.split('#',1);
            raw[1] = raw[1].strip();
            data = raw[1].strip().split();
            try:
                cur.execute("SET SEARCH_PATH to public;");
                sql = "DELETE FROM CUSTOMER WHERE cno = {}".format(data[0]);
                writeOutput("TASK "+x[0]+"\n");
                cur.execute(sql);
                table_df = pd.read_sql_query(sql, conn);
                table_str = table_df.to_string();
                writeOutput(table_str+"\n");
            except Exception as e:
                print(e);  
                
            try:
                cur.execute("SET SEARCH_PATH TO public;");
                sql="SELECT * FROM customer;"
                # Sending the query and connection object to read_sql_query method of pandas pd. It reurns table as dataframe.
                table_df=pd.read_sql_query(sql, conn)
                # Converting dataframe to string so that it can be written to text file.
                table_str=table_df.to_string()
                writeOutput(table_str+"\n")
            except Exception as e:
                print (e)     
                    
        if(x[0] == 'E'):
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split('#')
            bno = data[1]
            cno = data[0]
            qty = data[2]
            total_cost = 0
            try:
                cur.execute("SET SEARCH_PATH TO public;");

                cur.execute("\
                    INSERT INTO bookOrder (cno, bno, qty)\
                    VALUES ({}, {}, {});\
                ".format(cno, bno, qty));

                cur.execute("SELECT EXISTS (SELECT 1 FROM book WHERE bno = {});".format(bno))
                writeOutput("TASK "+x[0]+"\n")
                book_exists = cur.fetchone()[0]
                
                if not book_exists:
                    cur.execute("\
                        INSERT INTO book (bno, title, author, category, price, sales) \
                        VALUES ({}, 'Random Book', 'Random Author', 'Science', 20.00, 0); \
                    ".format(bno))

                    cur.execute("SELECT price FROM book WHERE bno = {};".format(bno))
#                   #https://www.geeksforgeeks.org/querying-data-from-a-database-using-fetchone-and-fetchall/
                    book_price = cur.fetchone()[0]
                    
                    cur.execute("SELECT sales FROM book WHERE bno = {};".format(bno))
#                   #https://www.geeksforgeeks.org/querying-data-from-a-database-using-fetchone-and-fetchall/
                    book_sales = cur.fetchone()[0]                     
            
                cur.execute("SELECT EXISTS (SELECT 1 FROM customer WHERE cno = {});".format(cno));
                customer_exists = cur.fetchone()[0];
                
                if not customer_exists:
                    cur.execute("\
                        INSERT INTO customer (cno, name, address, balance)\
                        VALUES ({}, 'Random Name', 'Random Address', 0.00);\
                    ".format(cno));

                cur.execute("SELECT price FROM book WHERE bno = {};".format(bno))
#               #https://www.geeksforgeeks.org/querying-data-from-a-database-using-fetchone-and-fetchall/
                book_price = cur.fetchone()[0]
                total_cost = total_cost + (book_price * Decimal(qty));

                # Update the balance of the customer
                cur.execute("\
                        UPDATE customer\
                        SET balance = balance - {}\
                        WHERE cno = {};\
                    ".format(total_cost, cno));               

                
                #Update the sales of the book
                cur.execute("\
                    UPDATE book\
                    SET sales = sales + {}\
                    WHERE bno = {};\
                ".format(qty, bno));

            except Exception as e:
                    writeOutput(str(e)+"\n");              

                    
            try:
                cur.execute("SET SEARCH_PATH TO public;");
                sql="SELECT * FROM bookorder;"
                # Sending the query and connection object to read_sql_query method of pandas pd. It reurns table as dataframe.
                table_df=pd.read_sql_query(sql, conn)
                # Converting dataframe to string so that it can be written to text file.
                table_str=table_df.to_string()
                writeOutput(table_str+"\n")
            except Exception as e:
                print (e)        
                  

        if(x[0] == 'F'):
            raw = x.split('#', 1)
            raw[1] = raw[1].strip()
            data = raw[1].strip().split('#');
            try:
                cur.execute("Set SEARCH_PATH to public;");
                sql_balance = "SELECT balance FROM customer WHERE cno = %s;"
                
                sql = "\
                       UPDATE customer \
                       SET balance = balance + {} \
                       WHERE cno = {};".format(data[1], data[0]);
               
                writeOutput("TASK "+x[0]+"\n");

                cur.execute(sql)

                cur.execute("SELECT balance FROM customer WHERE cno = %s;", (data[0],))
                updated_balance = cur.fetchone()[0]
                #table_df = pd.read_sql_query(sql, conn)
                #table_str=table_df.to_string();
                #writeOutput(table_str+"\n")               
            except Exception as e:
                writeOutput(str(e)+"\n")
                
            try:
                cur.execute("SET SEARCH_PATH TO public;");
                sql = "SELECT * FROM customer;"
                table_df=pd.read_sql_query(sql, conn)
                # Converting dataframe to string so that it can be written to text file.
                table_str=table_df.to_string()
                writeOutput(table_str+"\n")
            except Exception as e:
                print (e)    
             
        if(x[0] == 'G'):
                raw = x.split("#",1)
                raw[1]=raw[1].strip()
                data = raw[1].split("#")   
                # Statement to insert data into the student table
                try:
                    cur.execute("SET SEARCH_PATH TO public;");
                    sql = "SELECT b.title as Book_Title, c.name as customer_name, c.address FROM customer c JOIN bookOrder bo ON c.cno = bo.cno JOIN book b ON bo.bno = b.bno WHERE b.title LIKE '{}' ORDER BY b.title, c.name;".format (data[0]);
                    writeOutput("TASK "+x[0]+"\n")            
                    cur.execute(sql)
                    table_df=pd.read_sql_query(sql, conn)
                    # Converting dataframe to string so that it can be written to text file.
                    table_str=table_df.to_string()
                    writeOutput(table_str+"\n")
                except Exception as e:
                    writeOutput(str(e)+"\n")
                    
        if(x[0] == 'H'):
                raw = x.split("#",1)
                raw[1]=raw[1].strip()
                data = raw[1].split("#")   
                # Statement to insert data into the student table
                try:
                    cur.execute("SET SEARCH_PATH TO public;");
                    sql = "\
                          SELECT distinct(c.name) AS customer_name,\
                          b.bno AS book_number,\
                          b.title AS book_title,\
                          b.author AS book_author\
                          FROM customer c JOIN bookOrder bo ON c.cno = bo.cno\
                          JOIN book b ON bo.bno = b.bno \
                          WHERE c.cno = {} \
                          ORDER BY b.bno; ".format (data[0])

                    writeOutput("TASK "+x[0]+"\n")            
                    cur.execute(sql)
                    table_df=pd.read_sql_query(sql, conn)
                    # Converting dataframe to string so that it can be written to text file.
                    table_str=table_df.to_string()
                    writeOutput(table_str+"\n")
                except Exception as e:
                    writeOutput(str(e)+"\n")
          
   
        if(x[0] == 'I'):
                raw = x.split("#")
                try:
                    cur.execute("SET SEARCH_PATH TO public;");
                    sql = "SELECT category,\
                    SUM(sales) AS total_books_sold,\
                    SUM(sales * price) AS total_sales_value \
                    FROM book \
                    GROUP BY category \
                    ORDER BY category;"

                    writeOutput("TASK "+x[0]+"\n")            
                    cur.execute(sql)
                    table_df=pd.read_sql_query(sql, conn)
                    # Converting dataframe to string so that it can be written to text file.
                    table_str=table_df.to_string()
                    writeOutput(table_str+"\n")
                except Exception as e:
                    writeOutput(str(e)+"\n")
                    
        if(x[0] == 'J'):
                raw = x.split("#")
                try:
                    cur.execute("SET SEARCH_PATH TO public;");
                    sql = "SELECT c.cno AS customer_number, \
                    c.name AS customer_name, \
                    SUM(bo.qty) AS total_copies_on_order \
                    FROM customer c JOIN bookOrder bo ON c.cno = bo.cno \
                    GROUP BY c.cno, c.name ORDER BY c.cno;"

                    writeOutput("TASK "+x[0]+"\n")            
                    cur.execute(sql)
                    table_df=pd.read_sql_query(sql, conn)
                    # Converting dataframe to string so that it can be written to text file.
                    table_str=table_df.to_string()
                    writeOutput(table_str+"\n")
                except Exception as e:
                    writeOutput(str(e)+"\n")
                    
     cur.close()  # Close the cursor
     conn.close()  # Close the connection                

except Exception as e:
   print(e)           






