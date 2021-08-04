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


if __name__ == "__main__":
    indexes = Indexes()
    indexes.create_database()
    indexes.createTable()
    print("Successfully created table")
