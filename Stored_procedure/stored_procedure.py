'''
@Author: Naziya
@Date: 2021-08-05
@Last Modified by: Naziya
@Last Modified : 2021-08-05
@Title : Program Aim is to use stored procedure.
'''

import mysql.connector 
from mysql.connector import cursor
import os
from LoggerFormat import logger
from dotenv import load_dotenv
load_dotenv()

class StoredProcedure:

    def create_connection(self):
        self.mydb = mysql.connector.connect(
            host = os.environ.get("HOST"),
            user = os.environ.get("USERNAME"),
            password = os.environ.get("PASSWORD"),
            database = "student"
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
            mycursor.execute("DROP DATABASE IF EXISTS student")
            mycursor.execute("CREATE DATABASE student")    
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
            student_record ='''CREATE TABLE student_info(
                                student_id int not null,
                                student_code int,
                                student_name varchar(150) not null,
                                subject varchar(150),
                                marks int,
                                phone varchar(45),
                                PRIMARY KEY (student_id)
                                )'''
            myCursor.execute("DROP TABLE IF EXISTS student_info")
            myCursor.execute(student_record)
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
            insert_formula = "INSERT into student_info (student_id,student_code, student_name,subject,marks,phone) VALUES (%s,%s,%s,%s,%s,%s)"
            insert_records = [(1,101,'Kinjal','English',68,'9875842651'),(2,102,'Dolly','physics',70,'9365842652'),(3,103,'Diana','maths',70,'9275844854'),
                               (4,104,'vanraj','maths',90,'9237584465'),(5,105,'komal','maths',85,'9875841655'),(6,106,'dhiraj','science',92,'9577584965'),
                               (7,107,'piyush','science',83,'9875847654'),(8,108,'sanjay','science',85,'7275842657'),(9,109,'prachi','Biology',67,'8375832651')]
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
            myCursor.execute("SELECT * from student_info")
            result_set = myCursor.fetchall()
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def create_procedure(self):
        """
        Description: 
            This function is used to create procedure
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute('''
                                CREATE PROCEDURE get_merit_student() 
                                BEGIN  
                                SELECT * FROM student_info WHERE marks > 70;  
                                SELECT COUNT(student_code) AS Total_Student FROM student_info;
                                END 
                                ''')
            print("successfully created procedure\n")
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def call_procedure(self):
        """
        Description: 
            This function is used to call procedure
        Pararmeter:
            self is an instance of the object
        """
        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.callproc('get_merit_student')
            for result in myCursor.stored_results():
                print(result.fetchall())
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()




if __name__ == "__main__":
    storedProcedure = StoredProcedure()
    storedProcedure.create_database()
    storedProcedure.createTable()
    storedProcedure.insert_data()
    storedProcedure.show_data()
    storedProcedure.create_procedure()
    storedProcedure.call_procedure()