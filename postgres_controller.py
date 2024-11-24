import csv

import psycopg2
import random
from faker import Faker
import uuid
import datetime


class PostgresController:
    def __init__(self, db_name, db_user, db_password, db_host, db_port):
        self.dbname = db_name
        self.user = db_user
        self.password = db_password
        self.host = db_host
        self.port = db_port

        self.conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        self.faker = Faker()

    def generate_product(self):
        return {
            "Id": str(uuid.uuid4()),
            "CategoryId": random.randint(1, 100),
            "Name": self.faker.word(),
            "Price": random.randint(100, 10000),
            "Description": self.faker.word(),
            "Stock": random.randint(100, 100000),
            "ModifiedBy": random.randint(1, 10000),
            "ModifiedAt": datetime.datetime.now(),
            "IsDeleted": False
        }
    
    def generate_product_image(self, product_id):
        return {
            "Id": str(uuid.uuid4()),
            "ProductId": product_id,
            "Url": self.faker.image_url(),
            "IsDeleted": False,
        }
    
    def generate_product_spec(self, product_id):
        return {
            "Id": str(uuid.uuid4()),
            "ProductId": product_id,
            "SpecId": random.randint(1, 100),
            "Value": self.faker.word(),
        }
    
    def insert_product(self):
        product = self.generate_product()

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO products (Id, CategoryId, Name, Price, Description, Stock, ModifiedBy, ModifiedAt, IsDeleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    product["Id"],
                    product["CategoryId"],
                    product["Name"],
                    product["Price"],
                    product["Description"],
                    product["Stock"],
                    product["ModifiedBy"],
                    product["ModifiedAt"],
                    product["IsDeleted"]
                ))
            
            for _ in range(random.randint(1, 10)):
                product_image = self.generate_product_image(product["Id"])
                cur.execute("""
                    INSERT INTO product_images (Id, ProductId, Url, IsDeleted)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        product_image["Id"],
                        product_image["ProductId"],
                        product_image["Url"],
                        product_image["IsDeleted"]
                    ))
                
            for _ in range(random.randint(2, 20)):
                product_spec = self.generate_product_spec(product["Id"])
                cur.execute("""
                    INSERT INTO product_specs (Id, ProductId, SpecId, Value)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        product_spec["Id"],
                        product_spec["ProductId"],
                        product_spec["SpecId"],
                        product_spec["Value"]
                    ))
                
            self.conn.commit()

    def update_product(self):
        product_id = self.get_random_product_id()
        if product_id is None:
            return 0  # No products to update

        # Generate random update data
        updated_data = {
            "Price": round(random.uniform(10, 1000), 2),
            "Stock": random.randint(1, 100),
        }

        with self.conn.cursor() as cur:
            # Constructing the SQL query with dynamic fields
            query = "UPDATE products SET "
            query += ", ".join([f"{key} = %s" for key in updated_data])
            query += " WHERE id = %s"

            # Values for the query, ensuring the product ID is last
            values = list(updated_data.values()) + [product_id]

            cur.execute(query, values)
            self.conn.commit()
            return cur.rowcount

    # Returns the number of rows updated

    def delete_product(self):
        product_id = self.get_random_product_id()
        if product_id is None:
            return 0  # No products to delete

        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM products WHERE id = %s", (product_id,))
            self.conn.commit()
            return cur.rowcount  # Returns the number of rows deleted

    def read_product(self):
        product_id = self.get_random_product_id()
        if product_id is None:
            return None  # No products to read

        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM products WHERE id = %s", (product_id,))
            product = cur.fetchone()

            if product is None:
                return 0
            
            cur.execute("SELECT * FROM product_images WHERE product_images.ProductId = %s", (product_id,))
            images = cur.fetchall()

            cur.execute("SELECT * FROM product_specs WHERE product_specs.ProductId = %s", (product_id,))
            specs = cur.fetchall()

            return {
                "Product": product,
                "Images": images,
                "Specs": specs,
            }
        

    def get_random_product_id(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT Id FROM products ORDER BY RANDOM() LIMIT 1;")
            result = cur.fetchone()
            return result[0] if result else None

    def crud_all(self):
        self.insert_product()
        self.read_product()
        self.update_product()
        self.delete_product()

    def export_table_to_csv(self, table_name="products", output_file="new_import_postgres.csv"):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")

            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            with open(output_file, 'w', newline='') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(columns)  # Write the column headers
                csv_writer.writerows(rows)