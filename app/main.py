from libs.pysparkApi import pysparkAPI
from typing import Optional

app = pysparkAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/sessions/{user}/{session_id}")
def get_or_create_session(user: str, session_id: Optional[str] = None):
    if app.get_pyspark_session(session_id) is not None:
        for session in app.session_table:
            if session['session_id'] == session_id:
                return app.stat_pyspark_session(session_id)
    else:
        return {"session_id": app.init_pyspark_session(user)}

@app.delete("/sessions/{user}/{session_id}")
def delete_session(session_id: str, user: str):
    if app.delete_pyspark_session(session_id):
        return "Deleted"
    else:
        return {"session_id": None}

@app.post("/sessions/{user}/{session_id}/{exec_id}")
def execute_query(exec_id: str, user: str, session_id: str, query: str):
    if app.execute_pyspark_query(session_id, query) is not None:
        return {"exec_id": app.execute_pyspark_query(session_id, query)}
    else:
        return {"exec_id": None}

@app.get("/sessions/{user}/{session_id}/{exec_id}")
def get_query_result(exec_id: str, user: str, session_id: str):
    if app.get_pyspark_query_result(exec_id) is not None:
        return app.get_pyspark_query_result(exec_id).toPandas()
    else:
        return {"exec_id": None}

@app.delete("/sessions/{user}/{session_id}/{exec_id}")
def cancel_query(exec_id: str, user: str, session_id: str):
    if app.get_pyspark_query_result(exec_id) is not None:
        return {"exec_id": app.get_pyspark_query_result(exec_id).cancel()}
    else:
        return {"exec_id": None}
