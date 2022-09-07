from sqlalchemy import Column, Integer, Float, String, Date
from sqlalchemy.orm import column_property

from utils.base import Base

class TranscationRaw(Base):
    __tablename__ = "transaction_raw"

    id = Column(Integer, primary_key=True)
    order_id = Column(String(55))
    order_date = Column(Date)
    ship_date = Column(Date)
    ship_mode = Column(String(55))
    customer_id = Column(String(55))
    product_id = Column(String(55))
    sales = Column(Float)
    quantity = Column(Integer)
    discount = Column(Float)
    profit = Column(Float)
    transaction_id = column_property(
        order_id
        + "_" 
        + customer_id
        + "_" 
        + product_id
    )

class TranscationClean(Base):
    __tablename__ = "transaction_clean"

    id = Column(Integer, primary_key=True)
    order_id = Column(String(55))
    order_date = Column(Date)
    ship_date = Column(Date)
    ship_mode = Column(String(55))
    customer_id = Column(String(55))
    product_id = Column(String(55))
    sales = Column(Float)
    quantity = Column(Integer)
    discount = Column(Float)
    profit = Column(Float)
    transaction_id = column_property(
        order_id
        + "_" 
        + customer_id
        + "_" 
        + product_id
    )