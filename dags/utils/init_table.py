from utils.base import Base, engine
# Create the table in the database
def main():
    Base.metadata.create_all(engine)