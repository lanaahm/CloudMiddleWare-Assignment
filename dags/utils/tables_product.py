from sqlalchemy import cast, Integer, Column, String
from sqlalchemy.orm import column_property

from utils.base import Base

class ProductRaw(Base):
    __tablename__ = "product_raw"

    id = Column(Integer, primary_key=True)
    product_id = Column(String(55))
    product_name = Column(String(255))
    subcategory = Column(String(55))
    category = Column(String(55))
    transaction_id = column_property(
        product_id + "_" + product_name + "_" + subcategory + "_" + category
    )

class ProductClean(Base):
    __tablename__ = "product_clean"

    id = Column(Integer, primary_key=True)
    product_id = Column(String(55))
    product_name = Column(String(255))
    subcategory = Column(String(55))
    category = Column(String(55))
    transaction_id = column_property(
        cast(id, String) + "_" + product_id + "_" + product_name + "_" + subcategory + "_" + category
    )