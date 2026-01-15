import re
import hmac
import flask
import hashlib
from http import HTTPStatus
from functools import wraps
from datetime import datetime
from datetime import timedelta


def generate_hash(payment_id):
    return hmac.new(
        flask.current_app.config["SECRET_KEY"].encode(),
        str(payment_id).encode(),
        hashlib.sha256,
    ).hexdigest()


def get_gravatar_hash(emailAddress=None):
    """
    Returns the Gravatar hash based on the provided email address.

    :param emailAddress: The email address used to generate the Gravatar hash.
        If not provided, the function returns the hash for an empty string.
    :type emailAddress: str, optional

    :return: The Gravatar hash for the provided email address.
    :rtype: str
    """
    return hashlib.md5(emailAddress.lower().encode("utf-8")).hexdigest()


def rate_limit(max_requests=5, window_minutes=15):
    """
    Rate limiting decorator to prevent brute force attacks.
    Limits requests by IP address.
    """

    def decorator(f):
        attempts = {}

        @wraps(f)
        def wrapped(*args, **kwargs):
            if not flask.current_app.debug:  # Skip in debug mode
                ip = flask.request.remote_addr
                now = datetime.now()

                # Clean old attempts
                attempts.update(
                    {
                        k: v
                        for k, v in attempts.items()
                        if v["timestamp"]
                        > now - timedelta(minutes=window_minutes)
                    }
                )

                # Check current IP
                if ip in attempts:
                    if attempts[ip]["count"] >= max_requests:
                        return (
                            flask.jsonify(
                                {
                                    "error": "Too many attempts",
                                    "message": f"Please try again after {window_minutes} minutes",
                                }
                            ),
                            HTTPStatus.TOO_MANY_REQUESTS,
                        )
                    attempts[ip]["count"] += 1
                else:
                    attempts[ip] = {"count": 1, "timestamp": now}

            return f(*args, **kwargs)

        return wrapped

    return decorator


def is_valid_email(email):
    return re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email)


def sanitize_input(input_str):
    return re.sub(r"[<>]", "", input_str).strip()
