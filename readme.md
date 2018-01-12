
## 前置要求

- Python 3.5+
- Zookeeper 3.4+

### 安装依赖

```
[sudo] pip install -r requirements.txt
```

### 启动

启动Zookeeper

启动Master
```
cd server/master
python3 master_server.py
```

启动Volumn
```
cd server/volumn
python3 volumn_server.py
```

启动Client
```
cd client
python3 client.py
```

### 客户端命令

上传文件
```
upload path/to/file
```

下载文件
```
download filename
```

列出文件
```
ls
```
