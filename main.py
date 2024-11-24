import time

from mongodb_controller import MongoController  # Adjust the import according to your file structure
from postgres_controller import PostgresController  # Adjust the import according to your file structure

# Mongo DB connection details
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "store"
mongo_collection_name = "products"

# Postgres SQL connection details
pg_db_name = "products"
pg_user = "postgres"
pg_password = "123R"
pg_host = "localhost"
pg_port = "5432"

# Create controller instances
mongo_controller = MongoController(mongo_uri, mongo_db_name, mongo_collection_name)
pg_controller = PostgresController(pg_db_name, pg_user, pg_password, pg_host, pg_port)

# Insert records and measure time efficiency
def crud_mongo(n):
    for _ in range(n):
        mongo_controller.crud_all()


def crud_postgres(n):
    for _ in range(n):
        pg_controller.crud_all()


number_of_records = 100
# Measure Mongo DB insertion time
start_time = time.time()
crud_mongo(number_of_records)
mongo_duration = time.time() - start_time
print(f"Mongo CRUD {number_of_records} records took {mongo_duration} seconds.")

# Measure Postgres insertion time
start_time = time.time()
crud_postgres(number_of_records)
postgres_duration = time.time() - start_time
print(f"Postgres CRUD {number_of_records} records took {postgres_duration} seconds.")

print()

# Exporting data from MongoDB
export_start_time = time.time()
mongo_controller.export_collection_to_json()
mongo_export_duration = time.time() - export_start_time
print(f"Exporting from Mongo took {mongo_export_duration} seconds.")

# Exporting data from PostgreSQL
export_start_time = time.time()
pg_controller.export_table_to_csv()
pg_controller.export_table_to_csv(table_name="product_images")
pg_controller.export_table_to_csv(table_name="product_specs")
postgres_export_duration = time.time() - export_start_time
print(f"Exporting from Postgres took {postgres_export_duration} seconds.")