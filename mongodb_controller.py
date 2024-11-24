from pymongo import MongoClient
import datetime
import random
from faker import Faker
from bson import ObjectId
import json
import subprocess

class MongoController:
    def __init__(self, uri, db_name, collection_name):
        self.client = MongoClient(uri)
        self.uri = uri
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.faker = Faker()

    def generate_product(self):
        categories = [
                    {"id": 1, "name": "Laptops"},
                    {"id": 2, "name": "Graphics Cards"},
                    {"id": 3, "name": "CPU"},
                    {"id": 4, "name": "Motherboards"},
                    {"id": 5, "name": "Storage Devices"}
        ]
        category = random.choice(categories)

        product = {
            "name": self.faker.word(),
            "category": {
                "id": category["id"],
                "name": category["name"]
            },
            "price": random.randint(100, 10000),
            "description": self.faker.word(),
            "stock": random.randint(100, 100000),
            "images": [{"url": self.faker.image_url()} for _ in range(random.randint(1, 10))],
            "specs": [{"name": self.faker.word(), "value": self.faker.word()} for _ in range(random.randint(2, 20))],
            "modifiedAt": datetime.datetime.now(),
            "created_by": ObjectId(),
            "updated_by": ObjectId(),
            "isDeleted": False
        }
        return product

    def insert_product(self):
        self.collection.insert_one(self.generate_product())

    def delete_product(self):
        random_product = self.collection.find_one()  # Get a random product
        if random_product:
            self.db.products.delete_one({"_id": random_product["_id"]})

    def read_product(self):
        return self.collection.find_one()  # Returns a random product document

    def update_product(self):
        random_product = self.collection.find_one()  # Get a random product
        if random_product:
            updated_data = {
                "price": round(random.uniform(10, 1000), 2),
                "stock": random.randint(1, 100)
            }
            self.collection.update_one(
                {"_id": random_product["_id"]},
                {"$set": updated_data}
            )


    def crud_all(self):
        self.insert_product()
        self.read_product()
        self.update_product()
        self.delete_product()

    def export_collection_to_json(self, output_file="mongo_export.json"):
        collection = self.db['products']
        data = list(collection.find())

        with open(output_file, 'w') as file:
            json.dump(data, file, default=str)