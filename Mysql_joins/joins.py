'''
@Author: Naziya
@Date: 2021-08-04
@Last Modified by: Naziya
@Last Modified : 2021-08-04
@Title : Program Aim is to use different types of joins.
'''

import mysql.connector 
from mysql.connector import cursor
import os
from LoggerFormat import logger
from dotenv import load_dotenv
load_dotenv()

class Joins:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD"),
            database = "employee_service"
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
            mycursor.execute("DROP DATABASE IF EXISTS employee_service")
            mycursor.execute("CREATE DATABASE employee_service")    
            mycursor.execute("SHOW DATABASES")
            for db in mycursor:
                print(db)
        except Exception as err:
            logger.info(err)
        finally:
            self.mydb.close()


    def createTable1(self):
        """
        Description: 
            This function is used to create a table employee in a database.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            employee_record ='''CREATE TABLE employee(
                                employee_no int not null,
                                employee_name varchar(150) not null,
                                employee_designation varchar(150),
                                employee_salary int(10),
                                employee_mgr char(1),
                                dept_no char(2),
                                PRIMARY KEY (employee_no)
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


    def insert_data1(self):
        """
        Description: 
            This function is used to insert data into a employee table
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            insert_formula = "INSERT into employee (employee_no,employee_name,employee_designation,employee_salary,employee_mgr,dept_no) VALUES (%s,%s,%s,%s,%s,%s)"
            insert_records = [  (1,'aaa','salesman',7000,'2','10'),
                                 (2,'bbb','manager',17000,'3','10'),
                                  (3,'ccc','president',40000,None,'30'),
                                   (4,'ddd','clerk',5000,'5','20'),
                                    (5,'eee','manager',20000,'3','20'),
                                     (6,'fff','clerk',8000,'5',None)
                                ]
            myCursor.executemany(insert_formula,insert_records)
            self.mydb.commit()
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    
if __name__ == "__main__":
    joins = Joins()
    joins.create_database()
    joins.createTable1()
    joins.insert_data1()
    