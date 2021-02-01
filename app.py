from __future__ import print_function
from flask import Flask, render_template, make_response
from flask import redirect, request, jsonify, url_for

import io
import os
import uuid
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import numpy as np
import subprocess
import psutil

app = Flask(__name__)
app.secret_key = 's3cr3t'
app.debug = True
app._static_folder = os.path.abspath("templates/static/")

@app.route('/', methods=['GET'])
def index():
    title = 'Pivot Algo Trade'
    return render_template('layouts/index.html',
                           title=title)

@app.route('/startmethod', methods = ['POST'])
def trade_start_post():
    work = subprocess.Popen("python provit_algo_alpaca.py")
    return "finish"

@app.route('/stopmethod', methods = ['POST'])
def trade_stop_post():
    for proc in psutil.process_iter():
        if proc.name() == "python.exe":
            if proc.cmdline() == ['python', 'provit_algo_alpaca.py']:
                print("FIND+++++++++++++Check")
                proc.kill()
    return "finish"
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
