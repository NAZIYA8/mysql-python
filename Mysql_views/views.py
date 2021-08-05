'''
@Author: Naziya
@Date: 2021-08-05
@Last Modified by: Naziya
@Last Modified : 2021-08-05
@Title : Program Aim is to create views.
'''

import mysql.connector 
from mysql.connector import cursor
import os
from LoggerFormat import logger
from dotenv import load_dotenv
load_dotenv()

class Views:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD"),
            database = "employee_details"
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
            mycursor.execute("DROP DATABASE IF EXISTS employee_details")
            mycursor.execute("CREATE DATABASE employee_details")    
            mycursor.execute("SHOW DATABASES")
            for db in mycursor:
                print(db)
        except Exception as err:
            logger.error(err)
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
            employee_record ='''CREATE TABLE employees(
                                employee_id int not null,
                                employee_name varchar(150) not null,
                                employee_city varchar(150),
                                employee_age int,
                                employee_salary int(10),
                                PRIMARY KEY (employee_id)
                                )'''
            myCursor.execute("DROP TABLE IF EXISTS employees")
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
            insert_formula = "INSERT into employees (employee_id, employee_name,employee_city,employee_age,employee_salary) VALUES (%s,%s,%s,%s,%s)"
            insert_records = [  (1,'Kinjal','mumbai',25,18000,),
                                (2,'Dolly','Bangalore',28,22000),
                                (3,'John','Delhi',30,32000),
                                (4,'Tom','Hyderabad',24,20000)]
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
            myCursor.execute("SELECT * from employees")
            result_set = myCursor.fetchall()
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def create_view(self):
        """
        Description: 
            This function is used to create view from the table.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("CREATE VIEW employee_detail AS SELECT employee_name,employee_city,employee_age FROM employees ")
            print("successfully created view\n")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def display_view(self):
        """
        Description: 
            This function is used to display view.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT * from employee_detail ")
            result = myCursor.fetchall()
            print(result)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def update_view(self):
        """
        Description: 
            This function is used to update view.
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("ALTER VIEW employee_detail AS SELECT employee_name,employee_city,employee_salary FROM employees ")
            print("\n Updated view successfully")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

if __name__ == "__main__":
    views = Views()
    views.create_database()
    views.createTable()
    views.insert_data()
    views.show_data()
    views.create_view()
    views.display_view()
    views.update_view()
    views.display_view()