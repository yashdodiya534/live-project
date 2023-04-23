#import the snowflake connector library as sf
import snowflake.connector as sf

# Creating the context object
# -----------------------------------

print("1. Creating Snowflake Connection/Context Object")

# it is simple authentication - using user-id & pwd
sf_conn_obj = sf.connect(
    user = 'TOMSTARK331',
    password = 'GFG607YNmMj!',
    account = 'zc29401.ap-southeast-1',
    warehouse = 'COMPUTE_WH',
    database = 'test',
    schema = 'PUBLIC'
)

print("2. Connetion established successfully ")
print("2.1 Object => " , type(sf_conn_obj))
print("2.2 Account => " , sf_conn_obj.account)
print("2.3 Database => " , sf_conn_obj.database)
print("2.4 Schema => " , sf_conn_obj.schema)
print("2.5 Warehouse => " , sf_conn_obj.warehouse)
print("2.6 Application => " , sf_conn_obj.application)

# -------------------------------------------------
print("3. From context, getting the cursor object")
sf_cursor_obj = sf_conn_obj.cursor()

print("3.1 Object => " , type(sf_cursor_obj))

# -------------------------------------------------
print("4. Ready to execute a query on cursor object")
try:
    # execute any kind of query via execute method
    sf_cursor_obj.execute("select \
    current_database(), current_schema(), current_warehouse(), \
    current_version(), current_account(), current_client()")

    # Same cursor object help to fetch data
    one_row = sf_cursor_obj.fetchone()
    print("Current DB => ",one_row[0])
    print("Current Schema => ",one_row[1])
    print("Current Warehouse => ",one_row[2])
    print("Current Version => ",one_row[3])
    print("Current Account => ",one_row[4])
    print("Current Client => ",one_row[5])
finally:
    #closing the connection object
    sf_cursor_obj.close()
    
# closing the context object
sf_conn_obj.close()