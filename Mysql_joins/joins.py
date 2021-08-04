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

    def createTable2(self):
        """
        Description: 
            This function is used to create a table department in database.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            dept_record ='''CREATE TABLE dept(
                                dept_no char(2),
                                dept_name varchar(150) not null,
                                location varchar(150)
                                )'''
            myCursor.execute("DROP TABLE IF EXISTS dept")
            myCursor.execute(dept_record)
            myCursor.execute("SHOW TABLES")
            for tb in myCursor:
                print(tb)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def insert_data2(self):
        """
        Description: 
            This function is used to insert data into table
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            insert_formula = "INSERT into dept (dept_no,dept_name,location) VALUES (%s,%s,%s)"
            insert_records = [  ('10','sales','mumbai'),
                                 ('20','hr','delhi'),
                                  ('30','accounts','chennai'),
                                   ('40','production','bengaluru'),
                                ]
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
            print("\n")
            myCursor.execute("SELECT * from dept")
            result_set2 = myCursor.fetchall()
            #print(result_set)
            for data2 in result_set2:
                print(data2)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def inner_join(self):
        """
        Description: 
            This function is used to perform the inner join.
            It selects the records that have matching values in both tables.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT employee_name,location FROM employee INNER JOIN dept ON employee.dept_no = dept.dept_no")
            result_set = myCursor.fetchall()
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def left_outer_join(self):
        """
        Description: 
            This function is used to perform the left outer join.
            It gives all the values from left table along with the matching values
            from right table.If there are no matching values it returns null.
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT employee_name,dept_name FROM employee LEFT OUTER JOIN dept ON employee.dept_no = dept.dept_no")
            result_set = myCursor.fetchall()
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()


    
if __name__ == "__main__":
    joins = Joins()
    joins.create_database()
    joins.createTable1()
    joins.insert_data1()
    joins.createTable2()
    joins.insert_data2()
    joins.show_data()
    print("\nInner join")
    joins.inner_join()
    print("\n Left Outer Join")
    joins.left_outer_join()