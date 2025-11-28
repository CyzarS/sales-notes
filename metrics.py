import time
import os
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

ENV = os.getenv("ENV", "local")

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Duración de las peticiones HTTP",
    ["method", "path", "code", "env"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
)

RESPONSES_COUNTER = Counter(
    "http_responses_total",
    "Conteo de respuestas HTTP por código",
    ["code", "env"],
)

def metrics_middleware(app):
    def wrapper(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        start_time = time.time()
        status = {"code": "0"}
        def custom_start_response(status_line, headers, *args):
            status["code"] = status_line.split(" ", 1)[0]
            return start_response(status_line, headers, *args)
        resp = app(environ, custom_start_response)
        duration = time.time() - start_time
        code = status["code"]
        REQUEST_LATENCY.labels(method=method, path=path, code=code, env=ENV).observe(duration)
        RESPONSES_COUNTER.labels(code=code, env=ENV).inc()
        return resp
    return wrapper

def metrics_endpoint():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}
