# 1) Create connection to database
# 2) Create 2 tables in database
# 3) write data into database
# 4) Create sql queries
# 5) Save results
import config
import queries

import os

import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()


class DatabaseKernel:
    def __init__(self, host, user, password, auth_plugin):
        self.host = host
        self.user = user
        self.password = password
        self.auth_plugin = auth_plugin

    def create_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                auth_plugin=self.auth_plugin
            )
            print('Succesfull connected')
        except Error as err:
            print(f'An error {err} occured')
        return connection

    def create_database(self, query):
        connection = self.create_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            print("Database created succesefully")
        except Error as err:
            print(f"The error {err} occured")
        finally:
            connection.close()

    def execute_query(self, query, num_query):
        connection = self.create_connection()
        cursor = connection.cursor()  # нужно ли использовать try .. catch (или with) когда создаешь курсор? Ответ: нет, все норм
        try:
            cursor.execute(query)
            # connection.commit() # когда его нужно юзать?
            res_query = pd.DataFrame(cursor.fetchall())
            num_query = num_query + 1
            print("Query executed succesfully")
        except Error as err:
            print(f"The error {err} occured")
        finally:
            connection.close()
        return res_query, num_query

    @staticmethod
    def create_table(df, name):
        engine = create_engine(f'mysql+mysqldb://root:password@localhost/{config.db_name}')
        df.to_sql(name=name, con=engine, if_exists='replace', index=False)
        print(f'Table {name} created ... \n')

    @staticmethod
    def save_result(result, num_query, extention):
        if not os.path.exists("./results"):
            os.makedirs("./results")
        if extention == 'json':
            path = f"results/query_{num_query}.json"
            result.to_json(f"results/query_{num_query}.json")
        elif extention == 'xml':
            path = f"results/query_{num_query}.xml"
            result.to_xml(f"results/query_{num_query}.xml")
        print(f"File query_{num_query} saved in this path: {path}")


def main():
    dbk = DatabaseKernel(config.host, config.user, config.password, config.auth_plugin)

    dbk.create_database(queries.create_database_query)

    extention = input('Enter extention (JSON or XML): ').lower()
    table_name = input('Enter name file (without extention): ')
    students_df = pd.read_json('./' + table_name + f'.{extention}')
    dbk.create_table(students_df, table_name)

    table_name = input('Enter name file (without extention): ')
    rooms_df = pd.read_json('./' + table_name + f'.{extention}')
    dbk.create_table(rooms_df, table_name)

    num_query = 0
    res_query, num_query = dbk.execute_query(queries.first_query, num_query)
    dbk.save_result(res_query, num_query, extention)

    res_query, num_query = dbk.execute_query(queries.second_query, num_query)
    dbk.save_result(res_query, num_query, extention)

    res_query, num_query = dbk.execute_query(queries.third_query, num_query)
    dbk.save_result(res_query, num_query, extention)

    res_query, num_query = dbk.execute_query(queries.forth_query, num_query)
    dbk.save_result(res_query, num_query, extention)


if __name__ == '__main__':
    main()