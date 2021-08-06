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

class Mysql_Operations:

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
            student_record ='''CREATE TABLE addressbook (first_name varchar(150) not null,
                                last_name varchar(150) not null,
                                address varchar(400),
                                city varchar(20),
                                state varchar(20),
                                zip int unsigned,
                                phone_number int unsigned,
                                email_id varchar(100) not null)'''
            myCursor.execute("DROP TABLE IF EXISTS addressbook")
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
            insert_formula = "INSERT into addressbook (first_name,last_name,address,city,state,zip,phone_number,email_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            insert_records = [  ('Syeda','Naziya','Sh40','Hassan','Karnataka',25603,987654321,'ab@gmail.com'),
                                ('Ashima','Arora','sds34','Panaji','Goa',68230,987456691,'cd@gmail.com'),
                                ('Payal','Sharma','djs64','Hassan','Karnataka',63342,978556321,'ef@gmail.com'),
                                ('Dolly','Singh','sd5h','Panaji','Goa',43345,912356321,'jh@gmail.com'),
                                ('Manu','Chaturvedi','uimh56','Patna','Bihar',12463,969456321,'ij@gmail.com'),
                                ('Vanraj','Shah','sdir78','Panaji','Goa',89125,982216321,'kl@gmail.com'),
                                ('Kinjal','Verma','jyt25','Bangalore','Karnataka',65842,987493321,'mn@gmail.com')]
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
            myCursor.execute("SELECT * from addressbook")
            result_set = myCursor.fetchall()
            #print(result_set)
            for data in result_set:
                print(data)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def order_by(self):
        """
        Description: 
            This function is used to sort the records in ascending or descending.
        Pararmeter:
            self is an instance of the object
        """


        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT * FROM addressbook ORDER BY state DESC, city ASC")
            result_set = myCursor.fetchall()
            for x in result_set:
                print(x)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def group_by(self):
        """
        Description: 
            This function is used to fetch data from multiple records and group the result
            by one or more column
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT state, COUNT(*) FROM addressbook GROUP BY state")
            result_set = myCursor.fetchall()
            for x in result_set:
                print(x)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def like_percent_wildcard(self):
        """
        Description: 
            This function is used to show like percent wildcard search
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT first_name FROM addressbook where state LIKE 'Karn%'")
            result_set = myCursor.fetchall()
            for x in result_set:
                print(x)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def like_underscore_wildcard(self):
        """
        Description: 
            This function is used to show like underscore wildcard search
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT first_name FROM addressbook where city LIKE 'Pan_ji'")
            result_set = myCursor.fetchall()
            for x in result_set:
                print(x)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()

    def not_like(self):
        """
        Description: 
            This function is used to show like using not operator
        Pararmeter:
            self is an instance of the object
        """

        self.create_connection()
        try:
            myCursor = self.mydb.cursor()
            myCursor.execute("SELECT first_name FROM addressbook where state NOT LIKE 'Karn%'")
            result_set = myCursor.fetchall()
            for x in result_set:
                print(x)
        except Exception as err:
            logger.error(err)
        finally:
            self.mydb.close()


if __name__ == "__main__":
    operations = Mysql_Operations()
    operations.create_database()
    operations.createTable()
    print("\nSuccessfully created table")
    operations.insert_data()
    print("\nSuccessfully inserted data")
    operations.show_data()
    operations.order_by()
    print("Successfully Sorted data\n")
    operations.group_by()
    print("\n")
    operations.like_percent_wildcard()
    print("Successfully performed like percent wildcard search")
    operations.like_underscore_wildcard()
    print("Successfully performed like underscore wildcard search")
    operations.not_like()
    print("Successgully used like with NOT operator")