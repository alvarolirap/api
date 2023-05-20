from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import urllib.parse

params = urllib.parse.quote_plus('Driver={ODBC Driver 18 for SQL Server};Server=tcp:globanttest.database.windows.net,1433;Database=business;Uid=alirap;Pwd=Mi20151104.;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
database_uri = "mssql+pyodbc:///?odbc_connect=%s" % params
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = database_uri

db = SQLAlchemy(app)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    datetime = db.Column(db.String(255), nullable=False)
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=False)

class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(255), nullable=False)
    employees = db.relationship('Employee', backref='department', lazy=True)

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job = db.Column(db.String(255), nullable=False)
    employees = db.relationship('Employee', backref='job', lazy=True)

def validate_transaction_employee(data):
    # Perform data validation based on the data dictionary rules
    # Return True if the transaction is valid, False otherwise
    if 'name' not in data or 'datetime' not in data or 'department_id' not in data or 'job_id' not in data:
        return False
    return True

def validate_transaction_job(data):
    # Perform data validation based on the data dictionary rules
    # Return True if the transaction is valid, False otherwise
    if 'job' not in data:
        return False
    return True

def validate_transaction_department(data):
    # Perform data validation based on the data dictionary rules
    # Return True if the transaction is valid, False otherwise
    if 'department' not in data:
        return False
    return True

@app.route('/api/employee/get', methods=['GET'])
def get_employees():
    employees = Employee.query.all()
    data = [{'id': employee.id, 'name': employee.name, 'datetime': employee.datetime,
             'department_id': employee.department_id, 'job_id': employee.job_id}
            for employee in employees]
    return jsonify(data)

@app.route('/api/department/get', methods=['GET'])
def get_departments():
    departments = Department.query.all()
    data = [{'id': department.id, 'department': department.department} for department in departments]
    return jsonify(data)

@app.route('/api/job/get', methods=['GET'])
def get_jobs():
    jobs = Job.query.all()
    data = [{'id': job.id, 'job': job.job} for job in jobs]
    return jsonify(data)

@app.route('/api/employee/post', methods=['POST'])
def create_employees():
    data = request.json
    if isinstance(data, list):
        # Batch insert
        valid_transactions = []
        invalid_transactions = []

        for transaction in data:
            if validate_transaction_employee(transaction):
                valid_transactions.append(transaction)
            else:
                invalid_transactions.append(transaction)

        if valid_transactions:
            try:
                db.session.bulk_insert_mappings(Employee, valid_transactions)
                db.session.commit()
                response = {'message': 'Batch transactions inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'No valid transactions found'}
            status_code = 400

        if invalid_transactions:
            response['invalid_transactions'] = invalid_transactions

    else:
        # Single insert
        if validate_transaction_employee(data):
            try:
                new_employee = Employee(**data)
                db.session.add(new_employee)
                db.session.commit()
                response = {'message': 'Transaction inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'Invalid transaction'}
            status_code = 400

    return jsonify(response), status_code

@app.route('/api/job/post', methods=['POST'])
def create_jobs():
    data = request.json
    if isinstance(data, list):
        # Batch insert
        valid_transactions = []
        invalid_transactions = []

        for transaction in data:
            if validate_transaction_job(transaction):
                valid_transactions.append(transaction)
            else:
                invalid_transactions.append(transaction)

        if valid_transactions:
            try:
                db.session.bulk_insert_mappings(Job, valid_transactions)
                db.session.commit()
                response = {'message': 'Batch transactions inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'No valid transactions found'}
            status_code = 400

        if invalid_transactions:
            response['invalid_transactions'] = invalid_transactions

    else:
        # Single insert
        if validate_transaction_job(data):
            try:
                new_job = Job(**data)
                db.session.add(new_job)
                db.session.commit()
                response = {'message': 'Transaction inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'Invalid transaction'}
            status_code = 400

    return jsonify(response), status_code

@app.route('/api/department/post', methods=['POST'])
def create_departments():
    data = request.json
    if isinstance(data, list):
        # Batch insert
        valid_transactions = []
        invalid_transactions = []

        for transaction in data:
            if validate_transaction_department(transaction):
                valid_transactions.append(transaction)
            else:
                invalid_transactions.append(transaction)

        if valid_transactions:
            try:
                db.session.bulk_insert_mappings(Department, valid_transactions)
                db.session.commit()
                response = {'message': 'Batch transactions inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'No valid transactions found'}
            status_code = 400

        if invalid_transactions:
            response['invalid_transactions'] = invalid_transactions

    else:
        # Single insert
        if validate_transaction_department(data):
            try:
                new_department = Department(**data)
                db.session.add(new_department)
                db.session.commit()
                response = {'message': 'Transaction inserted successfully'}
                status_code = 201
            except Exception as e:
                db.session.rollback()
                response = {'message': str(e)}
                status_code = 400
        else:
            response = {'message': 'Invalid transaction'}
            status_code = 400

    return jsonify(response), status_code

if __name__ == '__main__':
    app.run()