from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import column_property

from utils.base import Base

class CustomerRaw(Base):
    __tablename__ = "customer_raw"
    
    id = Column(Integer, primary_key=True)
    customer_id = Column(String(55))
    customer_name = Column(String(55))
    segment = Column(String(55))
    city = Column(String(55))
    country = Column(String(55))
    state = Column(String(55))
    region = Column(String(55))
    postal_code = Column(Integer)
    transaction_id = column_property(
        customer_id + "_" + city + "_" + country + "_" + state + "_" + region
    )

class CustomerClean(Base):
    __tablename__ = "customer_clean"

    id = Column(Integer, primary_key=True)
    customer_id = Column(String(55))
    customer_name = Column(String(55))
    segment = Column(String(55))
    city = Column(String(55))
    country = Column(String(55))
    state = Column(String(55))
    region = Column(String(55))
    postal_code = Column(Integer)
    transaction_id = column_property(
        customer_id
        + "_"
        + city
        + "_"
        + country
        + "_"
        + state
        + "_"
        + region
    )