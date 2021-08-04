'''
@Author: Naziya
@Date: 2021-08-04
@Last Modified by: Naziya
@Last Modified : 2021-08-04
@Title : Program Aim is to add index to column.
'''

import mysql.connector 
from mysql.connector import cursor
import os
from LoggerFormat import logger
from dotenv import load_dotenv
load_dotenv()

class Indexes:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD"),
            database = "customer_service"
            )

    def create_database(self):
        """
        Description: 
            This function is used to create a database.
        Pararmeter:
            self is an instance of the object
        """

        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD")
            )
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute("DROP DATABASE IF EXISTS customer_service")
            mycursor.execute("CREATE DATABASE customer_service")    
            mycursor.execute("SHOW DATABASES")
            for db in mycursor:
                print(db)
        except Exception as err:
            logger.info(err)
        finally:
            self.mydb.close()

    def createTable(self):
        """
        Description: 
            This function is used to create a table in a database.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            customer_record ='''CREATE TABLE customer(
                                customer_id int not null,
                                customer_name varchar(150) not null,
                                customer_city varchar(150),
                                customer_country varchar(150),
                                PRIMARY KEY (customer_id)
                                )'''
            myCursor.execute("DROP TABLE IF EXISTS customer")
            myCursor.execute(customer_record)
            myCursor.execute("SHOW TABLES")
            for tb in myCursor:
                print(tb)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def insert_data(self):
        """
        Description: 
            This function is used to insert data into a database
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            insert_formula = "INSERT into customer (customer_id, customer_name,customer_city, customer_country) VALUES (%s,%s,%s,%s)"
            insert_records = [  ('1','Kinjal','Bangalore','India'),
                                ('2', 'Dolly','Michigan','USA'),
                                ('3','John','Mysore','India'),
                                ('4','Tom','Toronto','Canada')]
            myCursor.executemany(insert_formula,insert_records)
            self.mydb.commit()
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()
    
    def show_data(self):
        """
        Description: 
            This function is used to show the data.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()    
            myCursor.execute("SELECT * from customer")
            result_set = myCursor.fetchall()
            #print(result_set)
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def singleColumnIndex(self):
        """
        Description: 
            This function is used to create index on a single column.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("CREATE INDEX customer_name_idx ON customer (customer_name)")
            print("successfully created index\n")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def composite_index(self):
        """
        Description: 
            This function is used to create index on two or more columns.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("CREATE INDEX customer_country_idx ON customer(customer_city,customer_country)")
            print("successfully created index\n")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def display_all_indexes(self):
        """
        Description: 
            This function is used to display all indexes created.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SHOW INDEX FROM customer")
            result = myCursor.fetchall()
            print(result)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def drop_index(self):
        """
        Description: 
            This function is used to drop index from table.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("ALTER TABLE customer DROP INDEX customer_country_idx")
            self.mydb.commit()
            print("Successfully dropped index")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()


if __name__ == "__main__":
    indexes = Indexes()
    indexes.create_database()
    indexes.createTable()
    print("Successfully created table")
    indexes.insert_data()
    indexes.show_data()
    indexes.singleColumnIndex()
    indexes.composite_index()
    indexes.display_all_indexes()
    indexes.drop_index()
    indexes.display_all_indexes()