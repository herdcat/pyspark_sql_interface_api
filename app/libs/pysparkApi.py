from fastapi import FastAPI
from pyspark.sql import SparkSession
from time import time
import concurrent.futures

class pysparkAPI(FastAPI):
    def __init__(self):
        super().__init__()
        self.session_table = []
        self.exec_table = []
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def init_pyspark_session(self, user, cores = 1, memory = "1g"):
        session_id = "{0}-{1}".format(user, str(time()).split('.')[-1])
        self.session_table.append(
            {
            'session_id' : session_id, 
            'session' : SparkSession.builder.appName(session_id).config("spark.cores.max", cores).config("spark.driver.memory", memory).getOrCreate()
            }
            )
        return session_id
    
    def stat_pyspark_session(self, session_id):
        for session in self.session_table:
            if session['session_id'] == session_id:
                return "Active"
        return "Inactive"

    def get_pyspark_session(self, session_id):
        for session in self.session_table:
            if session['session_id'] == session_id:
                return session['session']
        return None

    def delete_pyspark_session(self, session_id):
        for session in self.session_table:
            if session['session_id'] == session_id:
                session['session'].stop()
                self.session_table.remove(session)
                return True
        return False

    def execute_pyspark_query(self, session_id, query):
        session = self.get_pyspark_session(session_id)
        exec_id = "{0}-{1}".format(session_id, str(time()).split('.')[-1])
        self.exec_table.append(
            {
            'exec_id' : exec_id,
            'thread' : self.executor.submit(session.sql, query)
            }
        )
        if session is not None:
            return exec_id
        return None
    
    def get_pyspark_query_result(self, exec_id):
        for exec in self.exec_table:
            if exec['exec_id'] == exec_id:
                return exec['thread'].result()
        return None
    
    def stat_pyspark_query(self, exec_id):
        for exec in self.exec_table:
            if exec['exec_id'] == exec_id:
                if exec['thread'].done():
                    return "Finished"
                elif exec['thread'].running():
                    return "Running"
                else:
                    return "Not Started"
    def cancel_pyspark_query(self, exec_id):
        for exec in self.exec_table:
            if exec['exec_id'] == exec_id:
                exec['thread'].cancel()
                return True
        return False