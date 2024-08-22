import sys
import os 
sys.path.append(os.path.abspath('D:\\Infogis_server'))

from app.database.connect import Base, engine
from app.models.user import User
from app.models.main_input import MainInput

def create_tables():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    create_tables()