from flask import Flask, render_template, request, jsonify
import json
import datetime

app = Flask(__name__)

# Sample balance data (replace this with your actual trading data)
sample_data = [
    {"date": "2024-11-01", "balance": 100},
    {"date": "2024-11-02", "balance": 110},
    {"date": "2024-11-03", "balance": 90},
    {"date": "2024-11-04", "balance": 120},
    {"date": "2024-11-05", "balance": 140},
    {"date": "2024-11-06", "balance": 130},
    {"date": "2024-11-07", "balance": 150}
]

@app.route('/')
def index():
    # Load the initial data
    return render_template('index.html', initial_data=json.dumps(sample_data))

@app.route('/update_data', methods=['POST'])
def update_data():
    # Extract new parameters from the frontend form submission
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    starting_balance = float(request.form.get('starting_balance'))

    # Generate or filter your trading data based on the provided dates and starting balance
    # For now, this example will return the same sample data
    filtered_data = [
        {"date": "2024-11-01", "balance": starting_balance},
        {"date": "2024-11-02", "balance": starting_balance * 1.05},
        {"date": "2024-11-03", "balance": starting_balance * 0.95},
        {"date": "2024-11-04", "balance": starting_balance * 1.2},
        {"date": "2024-11-05", "balance": starting_balance * 1.4},
        {"date": "2024-11-06", "balance": starting_balance * 1.3},
        {"date": "2024-11-07", "balance": starting_balance * 1.5}
    ]

    return jsonify(filtered_data)

if __name__ == '__main__':
    app.run(debug=True)
