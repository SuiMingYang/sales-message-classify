import json
import requests

resp=requests.post("http://193.112.97.123:5577/push_chatdata",data={"chat":json.dumps([{"owner_wxid": "wxid_6i0vjbz69xc812","realChatUsr": "wxid_6i0vjbz69xc812","createTime":"1553571148","msgType":"IMAGE","toUsr":"wxid_6i0vjbz69xc812","fromUsr":"10946234013@chatroom","content":"aa"}])})
