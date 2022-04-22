import csv
import database as db

PW = "Namit123"
ROOT = "root"
DB = "ecommerce_record"
LOCALHOST = "localhost"
connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB 
db.create_switch_database(connection, DB, DB)
print("Task 1-a executed successfully, DB Schema created successfully")

RELATIVE_CONFIG_PATH = '../config/'
CUSTOMERS = 'customers'
VENDOR = 'vendors'
ITEM = 'items'
ORDER = 'orders'

create_customers_table = """
    CREATE TABLE customers (
        customer_id varchar(10) PRIMARY KEY,
        customer_name varchar(50) NOT NULL,
        customer_email varchar(50) NOT NULL,
        customer_password varchar(20) NOT NULL,
        address varchar(50), 
        is_vendor tinyint(1) DEFAULT 0
    )
    """

create_items_table = """
    CREATE TABLE items (
        product_id varchar(50) NOT NULL PRIMARY KEY,
        product_name varchar(50) NOT NULL,
        product_description varchar(50) NOT NULL,
        product_price float(50) NOT NULL,
        emi_available varchar(10) NOT NULL,
        fk_vendor_id varchar(10) NOT NULL,
        CONSTRAINT `fk_vendor_id` FOREIGN KEY (`fk_vendor_id`) REFERENCES `customers` (`customer_id`)
    )
    """

create_orders_table = """
    CREATE TABLE orders (
        order_id int NOT NULL PRIMARY KEY,
        total_value float(20) NOT NULL,
        order_quantity int NOT NULL,
        reward_point int NOT NULL,
        vendor_id varchar(10) NOT NULL,
        customer_id varchar(10) NOT NULL,
        CONSTRAINT `vendor_id` FOREIGN KEY (`vendor_id`) REFERENCES `customers` (`customer_id`),
        CONSTRAINT `customer_id` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`)
    )
    """

create_vendors_table = """
    CREATE TABLE vendors (
        vendor_id varchar(10) PRIMARY KEY,
        vendor_name varchar(50) NOT NULL,
        vendor_email varchar(50) NOT NULL,
        vendor_password varchar(20) NOT NULL
    )
    """

create_customer_leaderboard = """
    CREATE TABLE customer_leaderboard (
        customer_id varchar(50) NOT NULL PRIMARY KEY,
        total_value float(50) NOT NULL,
        customer_name varchar(50) NOT NULL,
        customer_email varchar(50) NOT NULL,
        CONSTRAINT `fk_customer_id` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`)
    )
    """


# Creating DB Tables
db.create_insert_query(connection, create_customers_table)
db.create_insert_query(connection, create_items_table)
db.create_insert_query(connection, create_orders_table)
db.create_insert_query(connection, create_vendors_table)
db.create_insert_query(connection, create_customer_leaderboard)
print("Task 1-b executed successfully, DB Tables created successfully")


# Insert data into Tables
with open(RELATIVE_CONFIG_PATH + CUSTOMERS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)

query = "INSERT INTO customers (customer_id ,customer_name ,customer_email, " \
        "customer_password ,address) VALUES (%s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)


# Insert into vendors table
with open(RELATIVE_CONFIG_PATH + VENDOR + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    val.pop(0)

query = "INSERT INTO vendors (vendor_id, vendor_name, vendor_email, vendor_password) VALUES (%s, %s, %s, %s)"
db.insert_many_data(connection, query, val)


# insert into Orders Table
with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    val.pop(0)

query = "INSERT INTO orders (order_id, customer_id, vendor_id, total_value, " \
        "order_quantity, reward_point) VALUES (%s, %s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)


# insert into Items Table
with open(RELATIVE_CONFIG_PATH + ITEM + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)

query = "INSERT INTO items (product_id, product_name, product_price, product_description, " \
        "fk_vendor_id, emi_available) VALUES (%s, %s, %s, %s, %s, %s)"
db.insert_many_data(connection, query, val)

print("Inserted Data in DB Tables")
print("Task 2-a executed successfully, Data inserted in DB Tables successfully")
