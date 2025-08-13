import sqlite3
import csv
from collections import defaultdict, Counter

DB_PATH = 'shipment_database.db'

def insert_shipping_data_0(cursor):
    with open('data/shipping_data_0.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            origin = row['origin_warehouse']
            destination = row['destination_store']
            product = row['product']
            quantity = int(row['product_quantity'])

            
            cursor.execute("""
                INSERT INTO shipments (origin, destination)
                VALUES (?, ?)
            """, (origin, destination))
            shipment_id = cursor.lastrowid

            
            cursor.execute("""
                INSERT INTO shipment_products (shipment_id, product_name, quantity)
                VALUES (?, ?, ?)
            """, (shipment_id, product, quantity))


def insert_shipping_data_1_and_2(cursor):
    
    shipment_meta = {}  
    with open('data/shipping_data_2.csv', newline='') as f2:
        reader = csv.DictReader(f2)
        for row in reader:
            sid = row['shipment_identifier']
            origin = row['origin_warehouse']
            destination = row['destination_store']
            shipment_meta[sid] = (origin, destination)

    
    shipment_products = defaultdict(Counter)  
    with open('data/shipping_data_1.csv', newline='') as f1:
        reader = csv.DictReader(f1)
        for row in reader:
            sid = row['shipment_identifier']
            product = row['product']
            shipment_products[sid][product] += 1

    
    for sid, product_counter in shipment_products.items():
        if sid not in shipment_meta:
            print(f"[WARN] Skipping unknown shipment_identifier: {sid}")
            continue

        origin, destination = shipment_meta[sid]

        cursor.execute("""
            INSERT INTO shipments (origin, destination)
            VALUES (?, ?)
        """, (origin, destination))
        shipment_id = cursor.lastrowid

        for product, quantity in product_counter.items():
            cursor.execute("""
                INSERT INTO shipment_products (shipment_id, product_name, quantity)
                VALUES (?, ?, ?)
            """, (shipment_id, product, quantity))


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Inserting shipping_data_0...")
    insert_shipping_data_0(cursor)

    print("Inserting shipping_data_1 and shipping_data_2...")
    insert_shipping_data_1_and_2(cursor)

    conn.commit()
    conn.close()
    print("Data insertion complete.")

if __name__ == "__main__":
    main()
