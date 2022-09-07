
from utils.base import session
from sqlalchemy import cast, Integer, Date, Float
from sqlalchemy.dialects.postgresql import insert

from utils.tables_customer import CustomerRaw, CustomerClean
from utils.tables_product import ProductRaw, ProductClean
from utils.tables_transaction import TranscationRaw, TranscationClean

def insert_customer_transactions():
    """
    Insert operation: add new customer data to the table clean
    """
    # Retrieve all the ids from the clean table
    clean_transaction_ids = session.query(CustomerClean.transaction_id)

    # date and price needs to be casted as their
    # datatype is not string but, respectively, Date and Integer
    transactions_to_insert = session.query(
        CustomerRaw.customer_id,
        CustomerRaw.city,
        CustomerRaw.country,
        CustomerRaw.state,
        CustomerRaw.region,
        CustomerRaw.segment,
        cast(CustomerRaw.postal_code, Integer),
        CustomerRaw.customer_name,
    ).filter(~CustomerRaw.transaction_id.in_(clean_transaction_ids))
	
    # Print total number of transactions to insert
    print("Transactions to insert:", transactions_to_insert.count())
    
    # Insert the rows from the previously selected transactions
    stm = insert(CustomerClean).from_select(
        ["customer_id", "city", "country", "state", "region", "segment", "postal_code", "customer_name"],
        transactions_to_insert,
    )

    # Execute and commit the statement to make changes in the database.
    session.execute(stm)
    session.commit()

def insert_product_transactions():
    """
    Insert operation: add new product data to the table clean
    """
    # Retrieve all the ids from the clean table
    clean_transaction_ids = session.query(ProductClean.transaction_id)

    # date and price needs to be casted as their
    # datatype is not string but, respectively, Date and Integer
    transactions_to_insert = session.query(
        ProductRaw.product_id,
        ProductRaw.product_name,
        ProductRaw.subcategory,
        ProductRaw.category,
    ).filter(~ProductRaw.transaction_id.in_(clean_transaction_ids))
	
    # Print total number of transactions to insert
    print("Transactions to insert:", transactions_to_insert.count())
    
    # Insert the rows from the previously selected transactions
    stm = insert(ProductClean).from_select(
        ["product_id", "product_name", "subcategory", "category"],
        transactions_to_insert,
    )

    # Execute and commit the statement to make changes in the database.
    session.execute(stm)
    session.commit()

def insert_transcation_transactions():
    """
    Insert operation: add new transcation data to the table clean
    """
    # Retrieve all the ids from the clean table
    clean_transaction_ids = session.query(TranscationClean.transaction_id)

    # date and price needs to be casted as their
    # datatype is not string but, respectively, Date and Integer
    transactions_to_insert = session.query(
        TranscationRaw.order_id,
        TranscationRaw.customer_id,
        TranscationRaw.product_id,
        cast(TranscationRaw.quantity, Integer),
        cast(TranscationRaw.sales, Float),
        cast(TranscationRaw.discount, Integer),
        cast(TranscationRaw.profit, Float),
        cast(TranscationRaw.order_date, Date),
        TranscationRaw.ship_mode,
        cast(TranscationRaw.ship_date, Date),
    ).filter(~TranscationRaw.transaction_id.in_(clean_transaction_ids))
	
    # Print total number of transactions to insert
    print("Transactions to insert:", transactions_to_insert.count())
    
    # Insert the rows from the previously selected transactions
    stm = insert(TranscationClean).from_select(
        ["order_id", "customer_id", "product_id", "quantity", "sales", "discount", "profit", "order_date", "ship_mode", "ship_date"],
        transactions_to_insert,
    )

    # Execute and commit the statement to make changes in the database.
    session.execute(stm)
    session.commit()

def delete_transactions(ObjectRaw, ObjectClean):
    """
    Delete operation: delete any row not present in the last snapshot
    """
    # Get all transaction ids
    raw_transaction_ids = session.query(ObjectRaw.transaction_id)

    # Filter all the table transactions that are not present in the table
    # and delete them.
    # Passing synchronize_session as argument for the delete method.
    transactions_to_delete = session.query(ObjectClean).filter(
        ~ObjectClean.transaction_id.in_(raw_transaction_ids)
    )
    
    # Print transactions to delete
    print("Transactions to delete:", transactions_to_delete.count())

    # Delete transactions
    transactions_to_delete.delete(synchronize_session=False)

    # Commit the session to make the changes in the database
    session.commit()

def main():
    print("[Load] Start")
    print("[Load] Inserting new rows data to all table (customer, product, transaction)")
    insert_customer_transactions()
    insert_product_transactions()
    insert_transcation_transactions()
    print("[Load] Deleting rows not available in the new transformed data")
    print("[Load] End")