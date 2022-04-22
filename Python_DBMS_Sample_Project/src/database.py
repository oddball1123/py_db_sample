import mysql.connector

# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySql DB conn successful")
    except Exception as e:
        print("Error: %s" % e)

    return connection


# This method will create the database
def create_switch_database(connection, db_name, switch_db):
    # For database creation use this method
    # If you have created your database using UI, no need to implement anything
    cursor = connection.cursor()
    try:
        drop_query = "DROP DATABASE IF EXISTS " + db_name
        db_query = "CREATE DATABASE " + db_name
        switch_query = "USE " + switch_db
        cursor.execute(drop_query)
        cursor.execute(db_query)
        cursor.execute(switch_query)
        print("DB Created successfully")
    except Exception as e:
        print("Error in Creating DB: %s" % e)


# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySql DB connection successful")
    except Exception as e:
        print("Error in establishing DB connection: %s" % e)

    return connection


# Perform all single insert statements in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query Successful")
    except Exception as e:
        print("Error in query: %s" % e)


# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print("Error in query: %s" % e)


# performing the execute many query over the table,
# this method will help us to inert multiple records using a single instance
def insert_many_data(connection, sql, val):
    # to perform multiple insert operation in the database
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query Successful")
    except Exception as e:
        print("Error in running multiple query: %s" % e)
