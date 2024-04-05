import snowflake.snowpark as snowpark
import snowflake.connector
from snowflake.snowpark.functions import col
import uuid, json
import random
import secrets
from datetime import datetime, timedelta,timezone

# the number of rows of data you want
number_of_rows = 2

def date():
   desired_date = datetime(2024, 1, 10, tzinfo=timezone.utc) + timedelta(days=random.randint(1, 10))

   # Generate random time within the day
   random_hours = random.randint(0, 23)
   random_minutes = random.randint(0, 59)
   random_seconds = random.randint(0, 59)
   random_microseconds = random.randint(0, 999999)

   # Create the datetime object with the random time
   created_at_value = desired_date.replace(
      hour=random_hours,
      minute=random_minutes,
      second=random_seconds,
      microsecond=random_microseconds
   )
   # formatted_created_at = created_at_value.strftime("%Y-%m-%d %H:%M:%S.%f %z")
   return created_at_value

def end_date():
   return date() + timedelta(days=random.randint(1, 10))

def start_date():
   return date() + timedelta(days=1)

# generate random version numbers for sample data
def generate_random_version():
   major = random.randint(1, 10)  # Adjust the range as needed
   minor = random.randint(0, 9)
   patch = random.randint(0, 9)
   
   return f'{major}.{minor}.{patch}'

def generate_random_fixed_length_hash():
   # Generate random bytes
   length = 40
   random_bytes = secrets.token_bytes(length // 2)

   # Convert bytes to hexadecimal
   hash_value = ''.join(format(byte, '02x') for byte in random_bytes)

   return hash_value

# return random but meaningful data of various types according to each column type
def generate_fake_data():
   WAREHOUSE_NAME   = f"WAREHOUSE_NAME_{random.randint(1, 100)}"
   QUERY_TYPE = random.choice(['RENAME_WAREHOUSE', 'CREATE_ROLE', 'GRANT', 'CREATE_TABLE', 'GET_FILES', 'REMOVE_FILES', 'SHOW', 'ALTER_WAREHOUSE_SUSPEND', 'DESCRIBE', 'SELECT', 'PUT_FILES', 'UNLOAD', 'CREATE_TASK', 'USE', 'SET', 'UNKNOWN', 'CREATE', 'CREATE_TABLE_AS_SELECT', 'ALTER', 'EXECUTE_TASK', 'CREATE_USER', 'LIST_FILES'])
   
   ERROR_CODE = random.choice([90082.0, 1003.0, 904.0, 90109.0, None, 2140.0, 1131.0, 711.0, 90230.0, 2141.0, 604.0, 393901.0, 2027.0, 2003.0, 2211.0, 2043.0, 630.0, 90105.0, 2049.0, 3540.0, 90106.0])
   
   return {
      'QUERY_ID':str(uuid.uuid4()),
      'QUERY_TEXT': 'QUERY',
      'SCHEMA_ID': random.randint(1, 2000),
      'SCHEMA_NAME':random.choice(['PUBLIC', 'ACCOUNT_USAGE', None]),
      'QUERY_TYPE': QUERY_TYPE,
      'SESSION_ID': random.randint(13312270001, 13312379999),
      'WAREHOUSE_NAME': WAREHOUSE_NAME,
      'QUERY_TAG': f"QUERY_TAG_{random.randint(1, 100)}",
      'EXECUTION_STATUS': random.choice(['FAIL', 'SUCCESS', 'INCIDENT', None]),
      'ERROR_CODE': ERROR_CODE,
      'START_TIME': start_date(),
      'TOTAL_ELAPSED_TIME': random.randint(1, 10439011),
      'PERCENTAGE_SCANNED_FROM_CACHE': round(random.uniform(0, 1), 10),
      'OUTBOUND_DATA_TRANSFER_CLOUD': random.choice(['AWS', 'AZURE', 'GCP', None]),
      'RELEASE_VERSION': generate_random_version(),
      'QUERY_HASH': generate_random_fixed_length_hash(),
      'QUERY_PARAMETERIZED_HASH':generate_random_fixed_length_hash(),
   }

# generate insert statements based of them and store them in insert_statements
def generate_insert_statements():
   insert_statements = []
   for _ in range(number_of_rows):
      fake_data = generate_fake_data()

      insert_statement = f"""
         INSERT INTO <DATABSE_NAME>.PUBLIC.<TABLE_NAME> (
               "QUERY_ID",
                ...
               "QUERY_PARAMETERIZED_HASH_VERSION"
         ) VALUES (
               
               '{fake_data["QUERY_ID"]}',
                ...
               '{fake_data["QUERY_PARAMETERIZED_HASH_VERSION"]}'
      );

      """
      insert_statements.append(insert_statement)
   return insert_statements

def main():
    insert_statements = generate_insert_statements()
    with snowflake.connector.connect(
        **{
            "account": "abcde.central-india.azure",
            "user": "Username",
            "role": "accountadmin",
            "password": "password",
            "warehouse": "default_warehouse",
        }
    ) as con:
        for statement in insert_statements:
            # execute all insert statements at once
            con.cursor().execute(statement)
            
main()
