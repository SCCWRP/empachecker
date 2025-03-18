import os
from flask import request, make_response


# Basic Authentication Function
def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid."""
    return username == 'sccwrp' and password == os.environ.get("APP_PW")

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return make_response(
        'This application is intended for an authorized user only.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    """Determines if the basic auth is correct"""
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated