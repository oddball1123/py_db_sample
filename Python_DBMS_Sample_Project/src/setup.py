import csv
import database as db

PW = "Namit123"
ROOT = "root"
DB = "ecommerce_record_new"
LOCALHOST = "localhost"
connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB 
db.create_switch_database(connection, DB, DB)
print("Task 1-a executed successfully, DB Schema created successfully")

# defining constants
RELATIVE_CONFIG_PATH = '../config/'
USER = 'users'
ITEM = 'items'
ORDER = 'orders'

# Creating the tables through python code here
create_items_table = """
    CREATE TABLE items (
        product_id varchar(45) NOT NULL PRIMARY KEY,
        product_name varchar(45) NOT NULL,
        product_price double NOT NULL,
        product_description varchar(100) NOT NULL,
        vendor_id varchar(10) NOT NULL,
        emi_available varchar(10) NOT NULL
        # CONSTRAINT `vendor_id` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`)
    )
    """

# order_id,customer_id,vendor_id,total_value,order_quantity,reward_point
create_orders_table = """
    CREATE TABLE orders (
        order_id int NOT NULL PRIMARY KEY,
        customer_id varchar(10) NOT NULL,
        vendor_id varchar(10) NOT NULL,
        total_value float(20) NOT NULL,
        order_quantity int NOT NULL,
        reward_point int NOT NULL
        # CONSTRAINT `vendor_id` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`),
        # CONSTRAINT `customer_id` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    )
    """

create_users_table = """
    CREATE TABLE users (
        user_id varchar(10) PRIMARY KEY,
        user_name varchar(50) NOT NULL,
        user_email varchar(50) NOT NULL,
        user_password varchar(20) NOT NULL,
        user_address varchar(50), 
        is_vendor tinyint(1) DEFAULT 0
    )
    """

create_customer_leaderboard = """
    CREATE TABLE customer_leaderboard (
        customer_id varchar(50) NOT NULL PRIMARY KEY,
        total_value float(50) NOT NULL,
        customer_name varchar(50) NOT NULL,
        customer_email varchar(50) NOT NULL
        # CONSTRAINT `customer_id` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    )
    """

db.create_insert_query(connection, create_users_table)
db.create_insert_query(connection, create_items_table)
db.create_insert_query(connection, create_orders_table)
db.create_insert_query(connection, create_customer_leaderboard)
print("Task 1-b executed successfully, DB Tables created successfully")


# Insert data into DB Tables
# inserting into items Table
with open(RELATIVE_CONFIG_PATH + ITEM + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)

query = "INSERT INTO items (product_id, product_name, product_price, product_description, " \
        "vendor_id, emi_available) VALUES (%s, %s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)

# inserting into orders Table
with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)

query = "INSERT INTO orders (order_id, customer_id, vendor_id, total_value, " \
        "order_quantity, reward_point) VALUES (%s, %s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)

# inserting into users Table
with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)

query = "INSERT INTO users (user_id, user_name, user_email, " \
        "user_password, user_address, is_vendor) VALUES (%s, %s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)

print("Inserted Data in DB Tables")
print("Task 2-a executed successfully, Data inserted in DB Tables successfully")
