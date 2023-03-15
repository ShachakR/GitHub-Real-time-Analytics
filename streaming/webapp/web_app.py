from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

def createReq2Plot(data):
    plt.cla()
    counts_by_language = {}
    batch_times = []
    for batch in data:
        batch_time = batch['batch_time']
        batch_counts = json.loads(json.dumps(batch['batch_counts']))
        batch_times.append(batch_time)
        
        for count in batch_counts:
            language = count['language']
            if language not in counts_by_language:
                counts_by_language[language] = []
            counts_by_language[language].append(count['count'])
    
    for language, counts in counts_by_language.items():
        plt.plot(batch_times, counts, label=language)

    plt.xlabel('Batch Time')
    plt.ylabel('Count')
    plt.title('Counts by Language for each Batch Interval')
    plt.legend()
    plt.savefig('/streaming/webapp/static/chart_req2.png')

    return '/static/chart_req2.png'


@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/getData', methods=['GET'])
def getData():
    r = Redis(host='redis', port=6379)
    data = r.get('data')

    try:
        data = json.loads(data)
    except TypeError:
        return jsonify({'msg': 'no data'})
    
    
    result = {
        'req1': data['req1'],
        'req2': createReq2Plot(data['req2'])
    }

    return jsonify(result)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')