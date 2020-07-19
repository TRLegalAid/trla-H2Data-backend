from flask import Flask, request, render_template
import pandas as pd

app = Flask(__name__)


@app.route('/welcome', methods=['GET'])
def welcome():
    return render_template('welcome.html')

if __name__ == '__main__':
    app.run(debug=True)
