from flask import current_app, g
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from werkzeug.local import LocalProxy

FLASK_SCOPED_SESSION_MAKERS = []
COMMIT_AFTER_REQUEST = []


def session_setup(app):
    """
    Args:
        app (Flask Application): The flask application to set up.

    Wires up any sessions created with `FS` to commit automatically once the request response is complete.
    This also handles removing the session on application context teardown.
    """

    @app.after_request
    def after_request_handler(response):
        is_error = 400 <= response.status_code < 600
        for do_commit, scoped in zip(COMMIT_AFTER_REQUEST, FLASK_SCOPED_SESSION_MAKERS):
            if do_commit and not is_error:
                scoped.commit()
        return response

    @app.teardown_appcontext
    def teardown_appcontext_handler(exception=None):
        for scoped in FLASK_SCOPED_SESSION_MAKERS:
            scoped.remove()


def FS(*args, **kwargs):
    """
    Args:
        args: Same arguments as SQLAlchemy's create_engine.
        kwargs: Same arguments as SQLAlchemy's create_engine.

    Returns:
        scoped_session: An SQLAlchemy scoped_session object.

    Creates a scoped session that's tied to the Flask application context.
    """

    commit_after_request = kwargs.pop("commit_after_request", True)

    def _scopefunc():
        if not hasattr(g, "flask_scoped_sessions"):
            g.flask_scoped_sessions = {}
        return id(g.flask_scoped_sessions)

    engine = create_engine(*args, **kwargs)
    s = scoped_session(
        sessionmaker(bind=engine),
        scopefunc=_scopefunc,
    )

    FLASK_SCOPED_SESSION_MAKERS.append(s)
    COMMIT_AFTER_REQUEST.append(commit_after_request)
    return s


class Proxies:
    def __getattr__(self, name):
        def get_proxy():
            return getattr(current_app, name)

        return LocalProxy(get_proxy)


proxies = Proxies()
