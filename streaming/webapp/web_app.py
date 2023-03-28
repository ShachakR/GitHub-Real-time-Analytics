from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

def createReq2Plot(data):
    plt.cla()
    
    batch_times = []
    counts_by_language = {}

    for repo in data:
        batch_time = repo['batch_time']
        language = repo['language']
        count = repo['count']

        if batch_time not in batch_times:
            batch_times.append(batch_time)

        if language not in counts_by_language:
            counts_by_language[language] = []

        counts_by_language[language].append(count)
    
    for language, counts in counts_by_language.items():
        plt.plot(batch_times, counts, label=language, marker='o')

    plt.xlabel('Batch Time')
    plt.ylabel('Count')
    plt.title('Counts by language for each batch interval')
    plt.legend()
    plt.savefig('/streaming/webapp/static/chart_req2.png')

    return '/static/chart_req2.png'

def createReq3Plot(data):
    plt.cla()
    languages = []
    avg_stars = []

    for d in data:
        languages.append(d['language'])
        avg_stars.append(d['avg_stargazers_count'])

    plt.bar(languages, avg_stars)

    # Add labels and title
    plt.xlabel('Languages')
    plt.ylabel('Average number of stars')
    plt.title('Average number of stars by language')
    plt.savefig('/streaming/webapp/static/bar_req3.png')

    return '/static/bar_req3.png' 

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
        'req2': createReq2Plot(data['req2']),
        'req3': createReq3Plot(data['req3']),
        'req4': data['req4']
    }

    return jsonify(result)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')