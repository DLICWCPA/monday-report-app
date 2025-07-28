from flask import Flask, render_template, request, send_file
from datetime import datetime
from sync import generate_report
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_report', methods=['POST'])
def generate_report_route():
    start_date_str = request.form['start_date']
    end_date_str = request.form['end_date']

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        excel_buffer = generate_report(start_date, end_date)

        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=f'monday_report_{start_date}_to_{end_date}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        # This will help debug if something goes wrong on the server
        return str(e)

if __name__ == '__main__':
    # This part is for running on your local machine if you want to test
    # OnRender will use Gunicorn to run the app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
