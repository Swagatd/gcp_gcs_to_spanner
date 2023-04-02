from google.cloud import storage, spanner
import csv
from create_spanner_db_tbl import create_database

INSTANCE_ID = 'demospanner'
DATABASE_ID = 'demo'

# Set up Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket('gwbucket_566765')
blob1 = bucket.blob('Singers.csv')
blob2 = bucket.blob('Albums.csv')

# Read CSV file contents
csv_data1 = blob1.download_as_string().decode('utf-8')
csv_reader1 = csv.DictReader(csv_data1.splitlines())

csv_data2 = blob2.download_as_string().decode('utf-8')
csv_reader2 = csv.DictReader(csv_data2.splitlines())

# Define table schema
# create database and table
database_id, instance_id = create_database(INSTANCE_ID, DATABASE_ID)
print("Created database {} on instance {}".format(database_id, instance_id))

# Set up Cloud Spanner client
spanner_client = spanner.Client()
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)

table_name1 = 'Singers'
table_name2 = 'Albums'

# Insert data into Cloud Spanner table
with database.batch() as batch:
    for row in csv_reader1:
        columns = tuple(row.keys())
        values = [tuple(row.values())]
        try:
            batch.insert(
                table=table_name1,
                columns=columns,
                values=values
            )
        except Exception as e:
            print(f"Error inserting data into {table_name1}: {str(e)}")

    for row in csv_reader2:
        columns = tuple(row.keys())
        values = [tuple(row.values())]
        try:
            batch.insert(
                table=table_name2,
                columns=columns,
                values=values
            )
        except Exception as e:
            print(f"Error inserting data into {table_name2}: {str(e)}")

print('Inserted data.')
