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

class CrudOperations:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("host"),
            user = os.environ.get("username"),
            password = os.environ.get("password"),
            database = "addressbook_service"
            )

    def create_database(self):
        """
        Description: 
            This function is used to create a database.
        Pararmeter:
            self is an instance of the object
        """

        self.mydb = mysql.connector.connect(
            host = os.environ.get("host"),
            user = os.environ.get("username"),
            password = os.environ.get("password")
            )
        try:
            mycursor = self.mydb.cursor()
            mycursor.execute("DROP DATABASE IF EXISTS addressbook_service")
            mycursor.execute("CREATE DATABASE addressbook_service")    
            mycursor.execute("SHOW DATABASES")
            for db in mycursor:
                print(db)
        except Exception as err:
            logger.info(err)
        finally:
            self.mydb.close()


if __name__ == "__main__":
    crud = CrudOperations()
    crud.create_database()