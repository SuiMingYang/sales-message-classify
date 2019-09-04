欢迎使用ShowDoc！
    
**简要描述：** 

- 数据导入接口

**请求URL：** 
- ` http://193.112.97.123:5577/push_chatdata `
  
**请求方式：**
- POST 

**参数：** 

|参数名|必选|类型|说明|
|:----    |:---|:----- |-----   |
|chat     |否  |string | 序列转字符串    |

 **返回示例**

``` 
  "success" #成功
  "error"   #失败
```


 **python示例** 

- requests.post("http://193.112.97.123:5577/push_chatdata",data={"chat":json.dumps([{'owner_wxid':'aa','realChatUsr':'aa','createTime':'aa','msgType':'aa','toUsr':'aa','fromUsr':'aa','content':'aa'}])})

**原excel参数：** 

|参数名|必选|类型|说明|
|:----    |:---|:----- |-----   |
|owner_wxid     |否  |string |     |
|realChatUsr     |否  |string |     |
|createTime     |否  |string |     |
|msgType     |否  |string |     |
|toUsr     |否  |string |     |
|fromUsr     |否  |string |     |
|content  |否 | string   |     |


