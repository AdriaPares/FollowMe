#!/usr/local/bin/python3

# Also requires to change permissions to be executed. chmod a+x wsgi.py

"""Application entry point."""
from application import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
