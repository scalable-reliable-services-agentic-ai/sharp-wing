import threading
from wsgiref.simple_server import make_server
from prometheus_client import make_wsgi_app, REGISTRY


def start_metrics_server(port=8000):
    """Start a Prometheus metrics server in a background thread."""

    def run_server():
        app = make_wsgi_app(REGISTRY)
        httpd = make_server("", port, app)
        httpd.serve_forever()

    thread = threading.Thread(target=run_server)
    thread.daemon = True
    thread.start()
