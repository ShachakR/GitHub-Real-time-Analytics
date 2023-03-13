from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/getData')
def getData():
    r = Redis(host='redis', port=6379)
    data = r.get('data')

    try:
        data = json.loads(data)
    except TypeError:
        return jsonify({'msg': 'no data'})
    
    return jsonify(data)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')