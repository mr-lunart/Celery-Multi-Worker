from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_engine(conn_params:dict):
    
    _connection_string = "postgresql+psycopg2://{}:{}@{}/{}".format(
        conn_params["user"], 
        conn_params["password"], 
        conn_params["host"], 
        conn_params["database"]
    )
    engine = create_engine(_connection_string)
    # engine = create_engine(_connection_string, pool_pre_ping=True, pool_recycle=3600)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    
    return session