#!flask/bin/python
import pandas as pd
from sqlalchemy import create_engine
from flask import Flask, jsonify
from flask import request,Response
import requests
import json

app = Flask(__name__)

@app.route('/push_chatdata', methods=['POST'])
def push_chatdata():
    try:
        info_list=request.form["chat"]
        #print(info_list.replace(' ',''))
        info_list=json.loads(info_list.replace(' ',''))
        try:
            df = pd.DataFrame(info_list)
            df.to_sql('chatdata',create_engine(""),if_exists="append",index=True)
            #df.to_sql('chatdata',create_engine("mysql+pymysql://root:smy123456@127.0.0.1/businessdata?charset=utf8"),if_exists="append")
            return "success"#json.dumps({'status':'success'})
        except Exception as e:
            print(e)
            return "error"#json.dumps({'status':'error','msg':e})
    except Exception as e:
        print(e)

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0',port=5577)