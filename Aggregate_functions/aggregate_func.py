'''
@Author: Naziya
@Date: 2021-08-07
@Last Modified by: Naziya
@Last Modified : 2021-08-07
@Title : Program Aim is to perform other operations.
'''

import mysql.connector 
from mysql.connector import cursor
import os
from LoggerFormat import logger
from dotenv import load_dotenv
load_dotenv()

class Aggregate_Functions:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD"),
            database = "employees"
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
            mycursor.execute("DROP DATABASE IF EXISTS employees")
            mycursor.execute("CREATE DATABASE employees")    
            mycursor.execute("SHOW DATABASES")
            for db in mycursor:
                print(db)
        except Exception as err:
            logger.info(err)
        finally:
            self.mydb.close()

if __name__ == "__main__":
    aggregate_function = Aggregate_Functions()
    aggregate_function.create_database()