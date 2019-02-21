import pyodbc
import json
from flask import Flask, request, Response
DB_CONN= pyodbc.connect('Driver={SQL Server};Server=LAPTOP-QDBHTJDQ\SHUBHAM;Database=speedy_taxi;')
DB_CUR = DB_CONN.cursor()

APP = Flask(__name__)

def get_data(data):
    """
    This function deserializes an JSON object.
    :param data: JSON data
    :type data: str
    """
    json_data = json.loads(data)
    print("Deserialized data: {}".format(data))
    return json_data
#def return_result(result):
 #   """
 #   This function simply returns an operation's status in JSON.
  # :param result: boolean whether successful
  #  :type result: bool
  #  """
    ret = {}
 #   if result:
 #       ret["code"] = 0
 #       ret["message"] = "SUCCESS"
 #   else:
 #       ret["code"] = 1
 #       ret["message"] = "FAILURE"
 #   return json.dumps(ret)

def driver_create(driver_id, df_name,dl_name,d_number,d_address):
    """
    This function creates an user.
    :param user_id: user ID
    :type user_id: int
    :param user_name: user name
    :type user_name: str
    :param user_mail: user mail
    :type user_mail: str
    """
   # global DB_CONN
   # global DB_CUR

    try:
        DB_CUR.execute(
            """INSERT INTO driver (driver_id, df_name,dl_name,d_number,d_address)
            VALUES (?, ?, ?, ?, ?)""",
            (driver_id, df_name,dl_name,d_number,d_address)
        )
        DB_CONN.commit()
        print(
            "Added driver #{} with first name={},last name={},number={},address={}".format(
                driver_id, df_name, dl_name, d_number, d_address
            )
            )
        return True
    except Exception as err:
        print("Unable to create driver #{} with first name={},last name={},number={},address={}: {}".format(
            driver_id, df_name,dl_name,d_number,d_address, err
        ))
        return False
#print(driver_create(2,'aman','gupta',98989,'staines'))

def driver_update(driver_id, driver_newid, df_name, dl_name,d_number,d_address):
    """
    This function updates an user.
    :param user_id: user ID
    :type user_id: int
    :param user_name: user name
    :type user_name: str
    :param user_mail: user mail
    :type user_mail: str
    """
    #global DB_CONN
    #global DB_CUR

    try:
        DB_CUR.execute(
            """UPDATE users SET driver_id=?, df_name=?, d_last=?, d_number=?, d_address=?
            WHERE user_id=?""", (
                driver_newid, df_name, dl_name, d_number,d_address
            )
        )
        DB_CONN.commit()
        print(
            "Updated driver #{} with id={},first name={},last name={}, number={}, address={}".format(
                driver_id, driver_newid, df_name, dl_name,d_number,d_address
            )
            )
        return True
    except Exception as err:
        print("Unable to update driver #{} with id={},first name={},last name={}, number={}, address={}: {}".format(
            driver_id, driver_newid, df_name, dl_name,d_number,d_address, err
        ))
        return False
#print(driver_update(2,2,'anuj','gupta',989,'feltham'))

def driver_remove(driver_id):
    """
    This function removes an user.
    :param user_id: user ID
    :type user_id: int
    """
    global DB_CONN
    global DB_CUR

    print("About to remove driver #{}".format(driver_id))
    try:
        DB_CUR.execute(
            "DELETE FROM driver WHERE driver_id=?",
            (driver_id,)
        )
        DB_CONN.commit()
        #check whether an user was removed
        if DB_CUR.rowcount > 0:
            print("Removed user #{}".format(driver_id))
            return True
        return False
    except Exception as err:
        print("Unable to remove user #{}: {}".format(
            driver_id, err
        ))
        return False
#print(driver_remove(2))
def driver_get(driver_id):
    """
    This function retrieves a user's information.
    :param user_id: user ID
    :type user_id: int
    """
    #global DB_CUR

    #execute database query
    if driver_id > 0:
        #return all users
        DB_CUR.execute(
            "SELECT * FROM driver WHERE driver_id=?;",
            (driver_id,)
        )
    else:
        #return one particular user
        DB_CUR.execute("SELECT * FROM driver;")
    json = {}
    results = []
    temp = {}
    # get _all_ the information
    for row in DB_CUR:
        temp[row[0]] = {}
        temp[row[0]]["id"] = row[0]
        temp[row[0]]["first_name"] = row[1]
        temp[row[0]]["last_name"] = row[2]
        temp[row[0]]["number"] = row[3]
        temp[row[0]]["address"] = row[4]
        results.append(temp[row[0]])
    json["results"] = results
    return json

#print(driver_get(1))


def driver_complain(complain_id, complain_reason,complain_date,driver_id):
    try:
        DB_CUR.execute(
            """INSERT INTO complain (complain_id, complain_reason,complain_date,driver_id)
            VALUES (?, ?, ?, ?)""",
            (complain_id, complain_reason,complain_date,driver_id)
        )
        DB_CONN.commit()
        print(
            "Added driver complain #{} with reason={},complain_date={},id={}".format(
                complain_id, complain_reason,complain_date,driver_id
            )
            )
        return True
    except Exception as err:
        print("Unable to create driver complain #{} with reason={},complain_date={},id={}: {}".format(
            complain_id, complain_reason, complain_date, driver_id, err
        ))
        return False
#print(driver_complain(1,'bad','01/02/2019',1))
def complain_update(complain_id, complain_newid, complain_reason, complain_date,driver_id):
    """
    This function updates an user.
    :param user_id: user ID
    :type user_id: int
    :param user_name: user name
    :type user_name: str
    :param user_mail: user mail
    :type user_mail: str
    """
    #global DB_CONN
    #lobal DB_CUR

    try:
        DB_CUR.execute(
            """UPDATE complain SET complain_id=?, complain_reason=?, complain_date=? , driver_id=?
            WHERE complain_id=?""", (
                complain_newid, complain_reason, complain_date,driver_id
            )
        )
        DB_CONN.commit()
        print(
            "Updated complain #{} with id={},reason={},date={},id={}".format(
                complain_id, complain_newid, complain_reason, complain_date,driver_id
            )
            )
        return True
    except Exception as err:
        print("Unable to update complain #{} with id={},reason={},date={},id={}: {}".format(
            complain_id, complain_newid, complain_reason, complain_date,driver_id, err
        ))
        return False
#print(complain_update(1,1,'smell','09/09/2019',1))
def complain_remove(complain_id):
    """
    This function removes an user.
    :param user_id: user ID
    :type user_id: int
    """
    #global DB_CONN
    #global DB_CUR

    print("About to remove complain #{}".format(complain_id))
    try:
        DB_CUR.execute(
            "DELETE FROM complain WHERE complain_id=?",
            (complain_id,)
        )
        DB_CONN.commit()
        #check whether an user was removed
        if DB_CUR.rowcount > 0:
            print("Removed complain #{}".format(complain_id))
            return True
        return False
    except Exception as err:
        print("Unable to remove complain #{}: {}".format(
            complain_id, err
        ))
        return False
#print(complain_remove(1))

def complain_get(driver_id):
    """
    This function retrieves a user's information.
    :param user_id: user ID
    :type user_id: int
    """
    #global DB_CUR

    #execute database query
    if driver_id > 0:
        #return all users
        DB_CUR.execute(
            "SELECT * FROM complain WHERE complain_id=?;",
            (driver_id,)
        )
    else:
        #return one particular user
        DB_CUR.execute("SELECT * FROM complain;")
    json = {}
    results = []
    temp = {}
    # get _all_ the information
    for row in DB_CUR:
        temp[row[0]] = {}
        temp[row[0]]["id"] = row[0]
        temp[row[0]]["reason"] = row[1]
        temp[row[0]]["date"] = row[2]
        temp[row[0]]["driver_id"] = row[3]
        results.append(temp[row[0]])
    json["results"] = results
    return json
#print(complain_get(1))
def journey(journey_id, jstart_time,jend_time,duration_type,j_date,driver_id):
    try:
        DB_CUR.execute(
            """INSERT INTO journey (journey_id, jstart_time,jend_time,duration_type,j_date,driver_id)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (journey_id, jstart_time,jend_time,duration_type,j_date,driver_id)
        )
        DB_CONN.commit()
        print(
            "Added driver journey #{} with start_time={},end_time={},duration={},date={},driver_id={}".format(
                journey_id, jstart_time,jend_time,duration_type,j_date,driver_id
            )
            )
        return True
    except Exception as err:
        print("Unable to create journey #{} with start_time={},end_time={},duration={},date={},driver_id={}: {}".format(
            journey_id, jstart_time,jend_time,duration_type,j_date,driver_id, err
        ))
        return False


def journey_update(journey_id,journey_newid, jstart_time,jend_time,duration_type,j_date,driver_id):
    try:
        DB_CUR.execute(
            """UPDATE journey SET journey_id=?, jstart_time=?, jend_time=? , duration_type=?,j_date=?,driver_id=?
            WHERE complain_id=?""", (
                journey_newid, jstart_time,jend_time,duration_type,j_date,driver_id
            )
        )
        DB_CONN.commit()
        print(
            "Updated journey #{} with id={},start_time={},end_time={},duration={},date={},driver_id={}".format(
                journey_id, journey_newid, jstart_time,jend_time,duration_type,j_date,driver_id
            )
            )
        return True
    except Exception as err:
        print("Unable to update journey #{} with id={},start_time={},end_time={},duration={},date={},driver_id={}: {}".format(
            journey_id, journey_newid, jstart_time,jend_time,duration_type,j_date,driver_id, err
        ))
        return False

def journey_remove(journey_id):
    print("About to remove journey #{}".format(journey_id))
    try:
        DB_CUR.execute(
            "DELETE FROM journey WHERE journey_id=?",
            (journey_id,)
        )
        DB_CONN.commit()
        # check whether an user was removed
        if DB_CUR.rowcount > 0:
            print("Removed journey #{}".format(journey_id))
            return True
        return False
    except Exception as err:
        print("Unable to remove journey #{}: {}".format(
            journey_id, err
        ))
        return False

def journey_get(journey_id):
    if journey_id > 0:
        #return all users
        DB_CUR.execute(
            "SELECT * FROM journey WHERE journey_id=?;",
            (journey_id,)
        )
    else:
        #return one particular user
        DB_CUR.execute("SELECT * FROM journey;")
    json = {}
    results = []
    temp = {}
    # get _all_ the information
    for row in DB_CUR:
        temp[row[0]] = {}
        temp[row[0]]["id"] = row[0]
        temp[row[0]]["start time"] = row[1]
        temp[row[0]]["end time "] = row[2]
        temp[row[0]]["duration"] = row[3]
        temp[row[0]]["date"] = row[4]
        temp[row[0]]["driver_id"] = row[3]
        results.append(temp[row[0]])
    json["results"] = results
    return json

def qualification(qualification_id,qualification_types,qualification_name):
    try:
        DB_CUR.execute(
            """INSERT INTO qualifcation (qualification_id,qualification_types,qualification_name)
            VALUES (?, ?, ?)""",
            (qualification_id,qualification_types,qualification_name)
        )
        DB_CONN.commit()
        print(
            "Added driver qualification #{} with qualification_types={},name={}".format(
                qualification_id,qualification_types,qualification_name
            )
            )
        return True
    except Exception as err:
        print("Unable to create qualification #{} with qualification_types={},name={}: {}".format(
            qualification_id,qualification_types,qualification_name, err
        ))
        return False

def qualification_update(qualification_id, qualification_newid, qualification_types,qualification_name):


    try:
        DB_CUR.execute(
            """UPDATE qualification SET qualification_id=?, qualification_types=?, qualification_name=?  
            WHERE qualification_id=?""", (
                qualification_newid, qualification_types,qualification_name
            )
        )
        DB_CONN.commit()
        print(
            "Updated qualification #{} with id={},qualification_types={},name={}".format(
                qualification_id, qualification_newid, qualification_types,qualification_name
            )
            )
        return True
    except Exception as err:
        print("Unable to update qualification #{} with id={},qualification_types={},name={}: {}".format(
            qualification_id, qualification_newid, qualification_types,qualification_name, err
        ))
        return False

def qualification_remove(qualification_id):
    print("About to remove journey #{}".format(qualification_id))
    try:
        DB_CUR.execute(
            "DELETE FROM qualification WHERE qualification_id=?",
            (qualification_id,)
        )
        DB_CONN.commit()
        # check whether an user was removed
        if DB_CUR.rowcount > 0:
            print("Removed qualification #{}".format(qualification_id))
            return True
        return False
    except Exception as err:
        print("Unable to remove qualification #{}: {}".format(
            qualification_id, err
        ))
        return False

def qualification_get(qualification_id):
    if qualification_id > 0:
        #return all users
        DB_CUR.execute(
            "SELECT * FROM qualification WHERE qualification_id=?;",
            (qualification_id,)
        )
    else:
        #return one particular user
        DB_CUR.execute("SELECT * FROM qualification;")
    json = {}
    results = []
    temp = {}
    # get _all_ the information
    for row in DB_CUR:
        temp[row[0]] = {}
        temp[row[0]]["id"] = row[0]
        temp[row[0]]["types"] = row[1]
        temp[row[0]]["name "] = row[2]

        results.append(temp[row[0]])
    json["results"] = results
    return json

def training(training_id,training_session):
    try:
        DB_CUR.execute(
            """INSERT INTO training (training_id,training_session)
            VALUES (?, ?)""",
            (training_id,training_session)
        )
        DB_CONN.commit()
        print(
            "Added driver training #{} with training_session={}".format(
                training_id,training_session
            )
            )
        return True
    except Exception as err:
        print("Unable to create training #{} with training_session={}: {}".format(
            training_id,training_session, err
        ))
        return False

def training_update(training_id,training_newid,training_session):


    try:
        DB_CUR.execute(
            """UPDATE training SET training_id=?, training_session=?  
            WHERE training_id=?""", (
                training_newid, training_session
            )
        )
        DB_CONN.commit()
        print(
            "Updated training #{} with id={},training_session".format(
                training_id, training_newid, training_session
            )
            )
        return True
    except Exception as err:
        print("Unable to update training #{} with id={},training_session: {}".format(
            training_id, training_newid, training_session, err
        ))
        return False

def training_remove(training_id):
    print("About to remove training #{}".format(training_id))
    try:
        DB_CUR.execute(
            "DELETE FROM training WHERE training_id=?",
            (training_id,)
        )
        DB_CONN.commit()
        # check whether an user was removed
        if DB_CUR.rowcount > 0:
            print("Removed training #{}".format(training_id))
            return True
        return False
    except Exception as err:
        print("Unable to remove training #{}: {}".format(
            training_id, err
        ))
        return False

def qualification_get(training_id):
    if training_id > 0:
        #return all users
        DB_CUR.execute(
            "SELECT * FROM training WHERE training_id=?;",
            (training_id,)
        )
    else:
        #return one particular user
        DB_CUR.execute("SELECT * FROM training;")
    json = {}
    results = []
    temp = {}
    # get _all_ the information
    for row in DB_CUR:
        temp[row[0]] = {}
        temp[row[0]]["id"] = row[0]
        temp[row[0]]["session"] = row[1]

        results.append(temp[row[0]])
    json["results"] = results
    return json

