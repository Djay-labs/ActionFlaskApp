from flask import Blueprint, make_response, jsonify, request

basic_app = Blueprint('basic', __name__ )


def hello_world():
    name = request.args.get("Name") or "World"
    return make_response(jsonify({"response": f"Hello {name}"}))


basic_app.add_url_rule(rule="/hello_world", view_func=hello_world, methods=["GET"])
