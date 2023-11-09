from flask import Blueprint, make_response, jsonify, request

from Actions.action import lambda_handler

basic_app = Blueprint('basic', __name__ )


def hello_world():
    name = {}

    name["body-json"] = request.files["body-json"]
    print(name)
    lambda_handler(name, {})
    return make_response(jsonify({"response": f"Hello {name}"}))


basic_app.add_url_rule(rule="/hello_world", view_func=hello_world, methods=["POST"])
