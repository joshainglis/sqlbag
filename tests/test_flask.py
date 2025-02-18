from flask import Flask
from sqlalchemy import text

from sqlbag.flask import FS, session_setup


def test_flask_integration(db):
    app = Flask(__name__)

    s = FS(db)
    s2 = FS(db)

    @app.route("/")
    def hello():
        s.execute(text("select 1"))
        s2.execute(text("select 2"))
        return "ok"

    session_setup(app)

    client = app.test_client()
    result = client.get("/")
    assert result.status_code == 200

    # TODO: should test this a lot more thoroughly
