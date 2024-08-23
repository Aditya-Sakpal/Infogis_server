### Infogis Server 

This is FastAPI based server for IFRS17 app . SQLAlchemy is used for ORM and PostgreSQL is used for database . The purpose of this server is to show perform some calculations based on the user input and provide the response . 


### Installation :
```bash
   git clone https://github.com/Aditya-Sakpal/Infogis_serve
```
```bash
   python -m venv venv 
```
```bash
   venv\Scripts\activate
```
```bash
   pip install -r requirements.txt
```
```bash
   Change the database url in app/utils/constants_n_credentials.py 
```
```bash
   uvicorn main:app --reload
```
