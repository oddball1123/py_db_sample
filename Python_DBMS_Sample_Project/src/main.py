import database as db
import pandas as pd

# Driver code
if __name__ == "__main__":

    PW = "Namit123"
    ROOT = "root"
    DB = "ecommerce_record_new"
    LOCALHOST = "localhost"

    # # create DB server connection
    # connection = db.create_server_connection(LOCALHOST, ROOT, PW)

    # create DB connection
    connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)

    # 2-b
    # Insert 5 records into orders table
    val = [('101', '6', '5', '36480', '2', '200'),
           ('102', '4', '5', '73291', '5', '100'),
           ('103', '2', '5', '89414', '3', '100'),
           ('104', '5', '4', '42084', '3', '100'),
           ('105', '1', '5', '43547', '1', '100')]
    query = "INSERT INTO orders (order_id, customer_id, vendor_id, total_value, " \
            "order_quantity, reward_point) VALUES (%s, %s, %s, %s, %s, %s)"
    db.insert_many_data(connection, query, val)
    print("Task 2-b executed successfully, 5 records inserted into orders Table \n")

    # 2-c
    # read all records from orders table and print on console
    print("Following are all records from the orders Table:")
    query = "SELECT * FROM ecommerce_record_new.orders"
    orders_data = db.select_query(connection, query)
    # pretty print orders data on the terminal
    print('order_id|customer_id|vendor_id|total_value|order_quantity|reward_point')
    for row in orders_data:
        print(*row, sep='|')

    print("Task 2-c executed successfully, all records from orders Table printed "
          "on console \n")

    # read orders Table data into dataframe for further processing
    columns = ['order_id', 'customer_id', 'vendor_id', 'total_value', 'order_quantity', 'reward_point']
    df = pd.DataFrame(orders_data, columns=columns)

    # 3-a
    # find and print max/min value order
    # print orders with max/min total_value
    print("Record with Max total_value is:")
    print(df.iloc[df['total_value'].idxmax()].to_dict())
    print("Record with Min total_value is:")
    print(df.iloc[df['total_value'].idxmin()].to_dict())
    print("Task 3-a executed successfully, record with max/min total_value from "
          "orders Table printed on console \n")

    # 3-b
    # Find and print all the order details with total_value
    # more than the average order value ordered
    print("Orders with total_value more than mean total_value are as following:")
    print(df.loc[df.total_value > df.total_value.mean()].to_string(index=False))
    print("Task 3-b executed successfully, records with total_value more than mean "
          "total_value from orders Table printed on console \n")

    # 3-c
    # Create a new table named customer_leaderboard(customer_id, total_value, customer_name,
    # customer_email) and insert the highest ordered purchase for each of the
    # registered customers. If there are no orders you can ignore that customer.

    # Table already created in setup.py
    # get data to insert from orders and customers Tables
    query = 'SELECT t.user_name, t.user_id, t.user_email, MAX(t.total_value) FROM ' \
            '(SELECT user.user_id, user.user_name, ord.total_value, user.user_email FROM ' \
            'ecommerce_record_new.users as user right join ecommerce_record_new.orders as ord ' \
            'on user.user_id = ord.customer_id) as t GROUP BY t.user_id ;'

    customer_orders_data = db.select_query(connection, query)

    # insert values into customer_leaderboard Table
    query = "INSERT INTO customer_leaderboard (customer_id, total_value, " \
            "customer_name, customer_email) VALUES (%s, %s, %s, %s)"
    db.insert_many_data(connection, query, customer_orders_data)
    print('Task 3-c executed successfully, row with max total_value for each customer_id '
          'inserted in customer_leaderboard Table')
