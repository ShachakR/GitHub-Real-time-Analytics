
import requests
import time
import json
import requests
import socket
import os

url = 'https://api.github.com/search/repositories?q=language:Java+language:JavaScript+language:Python&sort=updated&order=desc&per_page=50'

HOST = "0.0.0.0"   
PORT = 9999
TOKEN = "token" + os.getenv('TOKEN')
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    with conn:
        print("Connected... Starting sending data.")
        while True:
            try:
                response = requests.get(url, headers={"Authorization": TOKEN})
                if response.ok:
                    data = response.json()
                    repositories = data["items"]
                    for repository in repositories:
                        data = {
                            'id': repository["id"],
                            'name': repository["name"],
                            'language': repository["language"],
                            'description': repository["description"],
                            'stargazers_count': repository["stargazers_count"],
                            'pushed_at': repository["pushed_at"]
                        }
                        json_data = f"{json.dumps(data)}\n".encode()
                        conn.send(json_data)
                        print(f"Data sent:{json_data}")
                else:
                    print("Error:", response.status_code, response.reason)
                time.sleep(15)
            except KeyboardInterrupt:
                s.shutdown(socket.SHUT_RD)