from faker import Faker
import pandas as pd
import random
import uuid

fake = Faker()

# Generate 20 unique users
user_ids = [str(uuid.uuid4()) for _ in range(20)]
users = [{
    "user_id": uid,
    "name": fake.name(),
    "email": fake.email(),
    "signup_date": fake.date_between(start_date='-2y', end_date='today')
} for uid in user_ids]

users_df = pd.DataFrame(users)

# Generate 20 unique products
product_ids = [str(uuid.uuid4()) for _ in range(20)]
products = [{
    "product_id": pid,
    "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
    "category": random.choice(["Electronics", "Books", "Clothing", "Toys", "Home", "Sports"]),
    "price": round(random.uniform(10.0, 500.0), 2),
    "stock": random.randint(10, 100)
} for pid in product_ids]

products_df = pd.DataFrame(products)

# Generate 20 orders using existing user_ids and product_ids
orders = [{
    "order_id": str(uuid.uuid4()),
    "user_id": random.choice(user_ids),
    "product_id": random.choice(product_ids),
    "quantity": random.randint(1, 5),
    "order_date": fake.date_between(start_date='-1y', end_date='today')
} for _ in range(20)]

orders_df = pd.DataFrame(orders)

# Generate 20 clickstream events using existing user_ids and product_ids
clickstream = [{
    "click_id": str(uuid.uuid4()),
    "user_id": random.choice(user_ids),
    "product_id": random.choice(product_ids),
    "timestamp": fake.date_time_between(start_date='-1y', end_date='now')
} for _ in range(20)]

clickstream_df = pd.DataFrame(clickstream)


data_path='/home/victus/Fahad/DATA-Project/Ecommerce_Project/Real_time-Raw_data'

# Write each DataFrame
users_df.to_csv(f'{data_path}/users.csv',index=False)
products_df.to_csv(f'{data_path}/products.csv',index=False)
orders_df.to_csv(f'{data_path}/orders.csv',index=False)
clickstream_df.to_csv(f'{data_path}/clickstream.csv',index=False)

