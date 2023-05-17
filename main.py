from flask import Flask

from route import basic

app = Flask(__name__)

# Registering blueprints
app.register_blueprint(basic.basic_app, url_prefix='/basic')

if __name__ == "__main__":
    app.run(debug=True)
