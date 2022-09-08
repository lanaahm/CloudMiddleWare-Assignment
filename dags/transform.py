import os
import csv
from datetime import datetime

from utils.base import session
from utils.tables_customer import CustomerRaw
from utils.tables_product import ProductRaw
from utils.tables_transaction import TranscationRaw
from sqlalchemy import text

# set path data
base_path = os.path.abspath(__file__ + "/../../")
raw_path_customer = f"{base_path}/data/raw/Customer_ID_Superstore.csv"
raw_path_product = f"{base_path}/data/raw/Product_ID_Superstore.csv"
raw_path_transaction = f"{base_path}/data/raw/final_superstore.csv"

def transform_case(input_string):
    """
    Lowercase string fields
    """
    return input_string.lower()

def update_date(date_input):
    """
    Update date format from DD/MM/YYYY to YYYY-MM-DD
    """
    date_patterns = ["%d-%m-%Y", "%m-%d-%Y", "%Y-%d-%m", "%Y-%m-%d"]   
    for pattern in date_patterns:
        try:
            current_format = datetime.strptime(date_input, pattern)
            new_format = current_format.strftime("%Y-%m-%d")
            return new_format
        except:
            pass
    return date_input

def update_class(class_input):
    """
    Simplify the ship_mode field for potentialy future analysis, just return:
    - "Second" if string contains "Second class" substring etc.
    """
    class_input = transform_case(class_input)
    return class_input.replace("class", "").strip()

def update_number(price_input, decimal=False, discount=False):
    """
    Return number as integer or float:
    - "," to convert the number into float first (e.g. from "100,000.00" to "100000.00")
    """
    price_input = float(price_input.replace(",", ""))
    price_input = price_input * 100 if discount else price_input
    return int(price_input) if decimal else price_input

def truncate_table(table):
    """
    Ensure that "{table}" table is always in empty state before running any transformations.
    And primary key (id) restarts from 1.
    """
    session.execute(
        text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
    )
    session.commit()

def transform_new_data_customer():
    """
    Apply all transformations for each row in the Customer_ID_Superstore.csv file before saving it into database
    """
    with open(raw_path_customer, mode="r", encoding="windows-1252") as csv_file:
        # Read the new .csv ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our objects
        raw_objects_customer = []
        for row in reader:
            # Apply transformations and save as object
            raw_objects_customer.append(
                CustomerRaw(
                    customer_id=transform_case(row["customer_id"]),
                    customer_name=transform_case(row["customer_name"]),
                    segment=transform_case(row["segment"]),
                    city=transform_case(row["city"]),
                    country=transform_case(row["country"]),
                    state=transform_case(row["state"]),
                    region=transform_case(row["region"]),
                    postal_code=update_number(row["postal_code"]),
                )
            )
        # Bulk save all new processed objects and commit
        session.bulk_save_objects(raw_objects_customer)
        session.commit()

def transform_new_data_product():
    """
    Apply all transformations for each row in the Product_ID_Superstore.csv file before saving it into database
    """
    with open(raw_path_product, mode="r", encoding="windows-1252") as csv_file:
        # Read the new .csv ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our objects
        raw_objects_product = []
        for row in reader:
            # Apply transformations and save as object
            raw_objects_product.append(
                ProductRaw(
                    product_id=transform_case(row["product_id"]),
                    product_name=transform_case(row["product_name"]),
                    subcategory=transform_case(row["subcategory"]),
                    category=transform_case(row["category"]),
                )
            )
        # Bulk save all new processed objects and commit
        session.bulk_save_objects(raw_objects_product)
        session.commit()

def transform_new_data_transaction():
    """
    Apply all transformations for each row in the Product_ID_Superstore.csv file before saving it into database
    """
    with open(raw_path_transaction, mode="r", encoding="windows-1252") as csv_file:
        # Read the new .csv ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our objects
        raw_objects_transaction = []
        for row in reader:
            # Apply transformations and save as object
            raw_objects_transaction.append(
                TranscationRaw(
                    order_id=transform_case(row["order_id"]),
                    order_date=update_date(row["order_date"]),
                    ship_date=update_date(row["ship_date"]),
                    ship_mode=update_class(row["ship_mode"]),
                    customer_id=transform_case(row["customer_id"]),
                    product_id =transform_case(row["product_id"]),
                    sales=update_number(row["sales"]),
                    quantity=update_number(row["quantity"], decimal=True),
                    discount=update_number(row["discount"], discount=True),
                    profit=update_number(row["profit"]),
                )
            )
        # Bulk save all new processed objects and commit
        session.bulk_save_objects(raw_objects_transaction)
        session.commit()

def transfrom_customer():
    print("[Transform] Start")

    print("[Transform] ==================== customer =================")
    print("[Transform] Remove any old data from customer_raw table")
    truncate_table('customer_raw')
    print("[Transform] Transform new data available in customer_raw table")
    transform_new_data_customer()

def transfrom_product():
    print("[Transform] ==================== product =================")
    print("[Transform] Remove any old data from product_raw table")
    truncate_table('product_raw')
    print("[Transform] Transform new data available in product_raw table")
    transform_new_data_product()

def transfrom_transaction():
    print("[Transform] ==================== transaction =================")
    print("[Transform] Remove any old data from transaction_raw table")
    truncate_table('transaction_raw')
    print("[Transform] Transform new data available in transaction_raw table")
    transform_new_data_transaction()

    print("[Transform] End")