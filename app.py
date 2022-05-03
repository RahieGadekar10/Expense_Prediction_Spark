from flask import Flask, render_template, request
import os
import sys
from werkzeug.utils import secure_filename
import Model_Prediction
from utility import read_params

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'prediction_file'

@app.route('/' , methods = ['GET'])
def index():
    return render_template("index.html")

@app.route('/hello' , methods = ['GET','POST'])
def hello():
    if request.method == 'POST':
        file = request.files['file']
        filename = secure_filename(file.filename)
        if filename.endswith(".csv") :
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            config1 = read_params(config_path="config/params.yaml")
            modelpred = Model_Prediction.ModelPrediction(config=config1)
            modelpred.predict()
            return render_template("download.html")
        else :
            return ("Invalid File")

if __name__ == '__main__':
    app.run(debug=True)
