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
            employee_record ='''CREATE TABLE employee(  
                                name varchar(45) NOT NULL,    
                                occupation varchar(35) NOT NULL,    
                                working_date date,  
                                working_hours varchar(10)
                                )'''
            myCursor.execute("DROP TABLE IF EXISTS employee")
            myCursor.execute(employee_record)
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
            insert_formula = "INSERT into employee (name, occupation, working_date, working_hours) VALUES (%s,%s,%s,%s)"
            insert_records = [  ('Kinjal', 'Scientist', '2020-10-04', 12),  
                                ('Vanraj', 'Engineer', '2020-10-04', 10),  
                                ('Samar', 'Actor', '2020-10-04', 13),  
                                ('Manish', 'Doctor', '2020-10-04', 14),  
                                ('Dolly', 'Teacher', '2020-10-04', 12),  
                                ('Komal', 'Business', '2020-10-04', 11)]
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
            myCursor.execute("SELECT * from employee")
            result_set = myCursor.fetchall()
            #print(result_set)
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def count(self):
        """
        Description: 
            This function is used to count
        Pararmeter:
            self is an instance of the object
        """


        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT COUNT(name) FROM employee")
            result_set = myCursor.fetchall()
            print(result_set)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()


if __name__ == "__main__":
    aggregate_function = Aggregate_Functions()
    aggregate_function.create_database()
    aggregate_function.createTable()
    print("\nSuccessfully created table")
    aggregate_function.insert_data()
    print("\nSuccessfully inserted data")
    aggregate_function.show_data()
    print("Successfully Showed data\n")
    aggregate_function.count()
    print("Used count function")