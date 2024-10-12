import hashlib

from flask import Flask, jsonify, request

# MySQL imports
from flask_mysqldb import MySQL
from environments import DB

from email_validator import validate_email, EmailNotValidError

app = Flask(__name__)

app.config['MYSQL_USER'] = DB['DB_USER']
app.config['MYSQL_PASSWORD'] = DB['DB_PASSWORD']
app.config['MYSQL_DB'] = DB['DB_NAME']
mysql = MySQL(app)


# CLIENTS=================================================================================


@app.route('/clients/', methods=['POST'])
def create_client() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if "name" not in data or \
            "surname" not in data or \
            "email" not in data or \
            "password" not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = create_client_func(data=data)

    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Created', 'user_id': answer[1]}), 201


def email_validate(email: str) -> bool:
    try:
        validate_email(email)
    except EmailNotValidError:
        return False
    return True


def create_client_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if not email_validate(data['email']):
        return ['Failed']

    hashed_password = hashlib.md5(data['password'].encode()).hexdigest()

    insert = "INSERT INTO clients (name, surname, email, password) VALUES (%s, %s, %s, %s);"

    values = (data['name'], data['surname'], data['email'], hashed_password)

    cursor.execute(
        insert, values
    )
    connect.commit()
    cursor.execute(
        "SELECT id FROM clients WHERE name = %s AND surname = %s AND email = %s AND password = %s;",  values
    )
    client = cursor.fetchone()
    cursor.close()
    connect.close()
    return ['OK', client[0]]


@app.route("/clients/", methods=['PUT'])
def update_client() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if "client_id" not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = update_client_func(data=data)
    if answer[0] == "Failed":
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Accepted', 'client_id': data['client_id']}), 202


def update_client_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if 'email' in data and not email_validate(data['email']):
        return ['Failed']

    update = "UPDATE clients SET "
    for key, value in data.items():
        if key == 'password' or key == 'client_id':
            continue
        update += key + ' = "' + value + '", '

    if 'password' in data:
        update += 'password = "' + hashlib.md5(data['password'].encode()).hexdigest() + '"'
    else:
        update = update[:-3]

    update += ' WHERE id = "' + str(data['client_id']) + '";'

    cursor.execute(update)
    connect.commit()
    cursor.close()
    connect.close()

    return ['OK']


@app.route("/clients/", methods=['GET'])
def search_client() -> jsonify:
    data = {
        'client_id': request.args.get('client_id'),
        'name': request.args.get('name'),
        'surname': request.args.get('surname')
    }
    data = {key: val for key, val in data.items() if val}
    answer = search_client_func(data=data)
    return jsonify({'status': 'OK', 'clients': answer[1]}), 200


def search_client_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if 'email' in data and not email_validate(data['email']):
        return ['Failed']

    search = "SELECT id, name, surname FROM clients "
    where = 'WHERE '
    for key, value in data.items():
        if key == 'client_id':
            continue
        where += key + ' = "' + value + '" AND '

    where = where[:-5]
    search += where + ';'
    cursor.execute(search)
    clients = cursor.fetchall()
    cursor.close()
    connect.close()
    return ['OK', clients]


@app.route("/clients/", methods=['DELETE'])
def delete_client() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'client_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = delete_client_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'OK'}), 200


def delete_client_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    cursor.execute(
        "DELETE FROM clients where id = %s;", (data['client_id'],)
    )
    connect.commit()
    cursor.close()
    connect.close()
    return ['OK']


# EMPLOYEES=================================================================================


@app.route("/employees/", methods=['POST'])
def create_employee() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if "name" not in data or \
            "qualification" not in data or \
            "surname" not in data or \
            "email" not in data or \
            "password" not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = create_employee_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Created', 'employee_id': answer[1]}), 201


def create_employee_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if not email_validate(data['email']):
        return ['Failed']

    hashed_password = hashlib.md5(data['password'].encode()).hexdigest()

    insert = "INSERT INTO employees (name, surname, qualification, email, password) VALUES (%s, %s, %s, %s, %s);"

    values = (data['name'], data['surname'], data['qualification'], data['email'], hashed_password)

    cursor.execute(
        insert, values
    )
    connect.commit()
    cursor.execute(
        "SELECT id FROM employees WHERE name = %s AND surname = %s AND qualification = %s AND email = %s "
        "AND password = %s;", values
    )
    employee = cursor.fetchall()
    cursor.close()
    connect.close()
    return ['OK', employee]


@app.route("/employees/", methods=['PUT'])
def update_employee() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if "employee_id" not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = update_employee_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Accepted', 'employee_id': data['employee_id']}), 200


def update_employee_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if 'email' in data and not email_validate(data['email']):
        return ['Failed']

    update = "UPDATE employees SET "
    for key, value in data.items():
        if key == 'password' or key == 'employee_id':
            continue
        update += key + ' = "' + value + '", '

    if 'password' in data:
        update += 'password = "' + hashlib.md5(data['password'].encode()).hexdigest() + '"'
    else:
        update = update[:-3]

    update += ' WHERE id = "' + str(data['employee_id']) + '";'

    cursor.execute(update)
    connect.commit()
    cursor.close()
    connect.close()

    return ['OK']


@app.route("/employees/", methods=['GET'])
def search_employee() -> jsonify:
    data = {
        'employee_id': request.args.get('employee_id'),
        'qualification': request.args.get('qualification'),
        'name': request.args.get('name'),
        'surname': request.args.get('surname')
    }
    data = {key: val for key, val in data.items() if val}

    answer = search_employee_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'OK', 'employees': answer[1]}), 200


def search_employee_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if 'email' in data and not email_validate(data['email']):
        return ['Failed']

    search = "SELECT  id, name, surname, qualification FROM employees "
    where = 'WHERE '
    for key, value in data.items():
        if key == 'employee_id':
            continue
        where += key + ' = "' + value + '" AND '

    where = where[:-5]
    search += where + ';'
    cursor.execute(search)
    employees = cursor.fetchall()
    cursor.close()
    connect.close()
    return ['OK', employees]


@app.route("/employees/", methods=['DELETE'])
def delete_employee() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'employee_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = delete_employee_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'OK'}), 200


def delete_employee_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    cursor.execute(
        "DELETE FROM employees where id = %s;", (data['employee_id'],)
    )
    connect.commit()
    cursor.close()
    connect.close()
    return ['OK']


# SERVICES=================================================================================


@app.route("/services/", methods=['POST'])
def create_service() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'employee_id' not in data or 'name' not in data or 'price' not in data or \
            'execution_time' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = create_service_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Created', 'employee_id': data['employee_id'], 'service_id': answer[1]}), 201


def create_service_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    values = (data['name'], data['price'], data['execution_time'])

    cursor.execute(
        "SELECT id FROM employees WHERE id = %s;", (data['employee_id'],)
    )
    employee = cursor.fetchone()

    if employee is None or len(employee) == 0:
        return ["Failed"]

    cursor.execute(
        "INSERT INTO services (name, price, execution_time) VALUES (%s, %s, %s);", values
    )
    connect.commit()
    cursor.execute(
        "SELECT id FROM services WHERE name = %s AND price = %s AND execution_time = %s;", values
    )
    service = cursor.fetchone()
    cursor.close()
    connect.close()
    return ['OK', service[0]]


@app.route("/services/", methods=['PUT'])
def update_service() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'employee_id' not in data or 'service_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = update_service_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Accepted', 'employee_id': data['employee_id'],
                    'service_id': data['service_id']}), 200


def update_service_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    cursor.execute(
        "SELECT id FROM employees WHERE id = %s;", (data['employee_id'],)
    )
    employee = cursor.fetchone()

    if employee is None or len(employee) == 0:
        return ["Failed"]

    update = "UPDATE services SET "
    for key, value in data.items():
        if key == 'employee_id' or key == 'service_id':
            continue
        update += key + ' = "' + str(value) + '", '

    update = update[:-2]

    update += ' WHERE id = "' + str(data['service_id']) + '";'

    cursor.execute(update)
    connect.commit()
    cursor.close()
    connect.close()

    return ['OK']


@app.route("/services/", methods=['GET'])
def search_service() -> jsonify:
    data = {
        'name': request.args.get('name'),
        'execution_time': request.args.get('execution_time'),
        'price': request.args.get('price'),
    }
    data = {key: val for key, val in data.items() if val}

    answer = search_service_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400

    return jsonify({'status': 'OK', 'services': answer[1]}), 200


def search_service_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    search = "SELECT  * FROM services "
    where = 'WHERE '
    for key, value in data.items():
        if key == 'employee_id' or key == 'service_id':
            continue
        where += key + ' = "' + value + '" AND '

    where = where[:-5]
    search += where + ';'

    cursor.execute(search)
    services = cursor.fetchall()
    cursor.close()
    connect.close()
    services = list(services)
    for i in range(len(services)):
        services[i] = list(services[i])
        services[i][3] = str(services[i][3])
    return ['OK', services]


@app.route("/services/", methods=['DELETE'])
def delete_service() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'employee_id' not in data or 'service_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = delete_service_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'OK', 'employee_id': data['employee_id']}), 200


def delete_service_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    cursor.execute(
        "DELETE FROM services where id = %s;", (data['service_id'],)
    )
    connect.commit()
    cursor.close()
    connect.close()
    return ['OK']


# VISITS=================================================================================


@app.route("/visits/", methods=['POST'])
def create_visit() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'client_id' not in data or 'employee_id' not in data or 'service_id' not in data or \
            'date' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = create_visit_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Created', 'client_id': data['client_id'], 'visit_id': answer[1]}), 201


def check_tables(data: dict) -> bool:
    connect = mysql.connect
    cursor = connect.cursor()
    cursor.execute(
        "SELECT id FROM clients WHERE id = %s;", (data['client_id'], )
    )
    client = cursor.fetchone()

    if client is None or len(client) == 0:
        return False

    cursor.execute(
        "SELECT id FROM employees WHERE id = %s;", (data['employee_id'], )
    )
    employee = cursor.fetchone()

    if employee is None or len(employee) == 0:
        return False

    cursor.execute(
        "SELECT id FROM services WHERE id = %s;", (data['service_id'], )
    )
    service = cursor.fetchone()

    if service is None or len(service) == 0:
        return False
    return True


def create_visit_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()
    if not check_tables(data=data):
        return ["Failed"]

    values = (data['client_id'], data['employee_id'], data['service_id'], data['date'])

    cursor.execute(
        "INSERT INTO visits (client_id, employee_id, service_id, date) VALUES (%s, %s, %s, %s);", values
    )
    connect.commit()
    cursor.execute(
        "SELECT id FROM visits WHERE client_id = %s AND employee_id = %s AND service_id = %s "
        "AND date = %s;", values
    )
    visit = cursor.fetchone()
    cursor.close()
    connect.close()
    return ['OK', visit[0]]


@app.route("/visits/", methods=['PUT'])
def update_visit() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'client_id' not in data or 'visit_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = update_visit_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'Accepted', 'client_id': data['client_id'], 'visit_id': data['visit_id']}), 200


def update_visit_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    if not check_tables(data=data):
        return ["Failed"]

    update = "UPDATE visits SET "
    for key, value in data.items():
        if key == 'client_id' or key == "visit_id":
            continue
        update += key + ' = "' + str(value) + '", '

    update = update[:-2]

    update += ' WHERE id = "' + str(data['visit_id']) + '";'

    cursor.execute(update)
    connect.commit()
    cursor.close()
    connect.close()

    return ['OK']


@app.route("/visits/", methods=['GET'])
def search_visit() -> jsonify:
    data = {
        'client_id': request.args.get('client_id'),
        'employee_id': request.args.get('employee_id'),
        'service_id': request.args.get('service_id'),
        'visit_id': request.args.get('visit_id'),
        'date': request.args.get('date'),
    }
    data = {key: val for key, val in data.items() if val}

    answer = search_visit_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400

    return jsonify({'status': 'OK', 'visits': answer[1]}), 200


def search_visit_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    search = "SELECT * FROM visits "
    where = 'WHERE '
    for key, value in data.items():
        if key == 'visit_id':
            continue
        where += key + ' = "' + str(value) + '" AND '

    where = where[:-5]
    search += where + ';'

    cursor.execute(search)
    visits = cursor.fetchall()
    cursor.close()
    connect.close()
    return ['OK', visits]


@app.route("/visits/", methods=['DELETE'])
def delete_visit() -> jsonify:
    data = {key: val for key, val in request.get_json().items() if val != ''}
    if 'client_id' not in data or 'visit_id' not in data:
        return jsonify({'status': 'Bad Request'}), 400
    answer = delete_visit_func(data=data)
    if answer[0] == 'Failed':
        return jsonify({'status': 'Bad Request'}), 400
    return jsonify({'status': 'OK'}), 200


def delete_visit_func(data: dict) -> list:
    connect = mysql.connect
    cursor = connect.cursor()

    cursor.execute(
        "DELETE FROM visits where id = %s;", (data['visit_id'],)
    )
    connect.commit()
    cursor.close()
    connect.close()
    return ['OK']


if __name__ == '__main__':
    app.run(debug=True)
