import json
from flask import Flask
from flask import make_response
from scoring import get_score
from flask.globals import request
from settings import HTTP_SCORING_AUTH
from custom_exceptions import AppNotFoundException
from symbol import except_clause

 
app = Flask(__name__)


@app.route('/applications/<int:app_id>/score', methods=['GET'])
def scoring_view(app_id):
    if request.headers.get('score_headers') != HTTP_SCORING_AUTH:
        return make_response(json.dumps({"error": "Unauthorized"}), 401)
    try:
        score = get_score(app_id)
        return make_response(
            json.dumps(
                {
                        "app_id": app_id,
                        "score": score
                }
            ), 200
        )
    except AppNotFoundException as err:
        return make_response(json.dumps({"error": err}), 404)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port='5001', debug=True)   
