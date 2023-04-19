import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def send_data_to_s3(result_data, topic_ext):
    invoke_url = f'https://ltt3iaz1xk.execute-api.us-east-1.amazonaws.com/test/topics/0a2bc878981f.{topic_ext}'
    payload = json.dumps({'records': [{'value': result_data}]}, default=str)
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request('POST', invoke_url, headers=headers, data=payload)
    print(response.status_code)

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_data = dict(row._mapping)
                print(pin_data)
                send_data_to_s3(pin_data, 'pin')
            
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                print(geo_result)
                send_data_to_s3(geo_result, 'geo')

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                print(user_result)
                send_data_to_s3(user_result, 'user')
            
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


