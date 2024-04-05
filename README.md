# snowflake-populator

blog explanation for this repo - https://medium.com/@zriyansh/snowflake-connector-in-python-remotely-c4973a9f8164

I had a use case where I had to test a piece of software (built on top of Snowflake) against massive data (10+ tables with each table having several rows ranging from 10k — 2 million rows).
What did I do?

So I had to write a script to generate that many data lines, the data, although generated should be meaningful at the least. The internet failed to find a one-stop solution and hence I ended up writing a Python script myself.

The script uses the QUERY_HISTORY view of Snowflake and associated schema (data type of each column) and populates it with however number of row data you want.

Here’s a brief overview of the code.
1. various functions to get random (but meaningful) values of start_date, end_date, random version data, hash values, random strings, random fixed choices, etc
2. function generate_insert_statements() to write insert statements off of it.
3. main() to execute all the insert statements at once by connecting to the Snowflake account using snowflake.connector

What the code does, Priyansh?
generate random data → connect to Snowflake with the account and warehouse info you provided → populate your tables there.

## Prerequisites:
1. A separate Database (preferably).
2. Create your table(s) from the Snowflake SQL notebook itself.
3. Execute the code remotely

### Why not use the Snowflake SQL notebook itself?
The code to execute the above code will be slightly different and running there might incur with some cost as well, or, you might want to integrate running your Python code into your web or mobile app.

**Note:** The above approach works, but data ingestion to snowflake tables is prolonged (8k rows ingestion in 60 minutes)

## A SECOND APPROACH
Populating my local postgres DB, exporting the data in CSV / JSON file formats (in multiple of 50 MB each export), and then uploading them to Snowflake tables.
Here’s how to run the same code and instead of inserting the data to Snowflake, we will populate our local DB. This is blazing fast, as the rate of ingestion depends on the power of your processor only.
Make sure the database and tables are already created and of the right data types.

```
import psycopg2

# do not forget to change this number
number_of_rows = 75000

#establishing the connection
conn = psycopg2.connect(
   database="DATABASE_NAME", user='postgres', password='1234', host='127.0.0.1', port= '5432'
)

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

def generate_insert_statements():
    #same code as above

if __name__ == "__main__":
   insert_statements = generate_insert_statements()

   # Assuming you have a PostgreSQL connection object named `conn`
   with conn.cursor() as cursor:
      for statement in insert_statements:
         cursor.execute(statement)

   # Commit the changes
   conn.commit()
   # Close the connection
   conn.close()
```
