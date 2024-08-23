# This file is used to connect to the database and create a session to interact with the database

from sqlalchemy import create_engine, MetaData # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
from sqlalchemy.ext.declarative import declarative_base # type: ignore
import sys
import os 
sys.path.append(os.path.abspath('D:\\Infogis_server'))
from app.utils.constants_n_credentials import DB_URL



DB_URL = DB_URL
engine = create_engine(DB_URL)  
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
metadata = MetaData()