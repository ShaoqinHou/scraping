import threading
import time
from flask import Blueprint, Response, jsonify, request, render_template


class CaptchaManager:
    """Shared CAPTCHA state for collectors and web UI."""

    def __init__(self):
        self._lock = threading.Lock()
        self._image = None
        self._created_at = None
        self._code = None
        self._event = threading.Event()

    def set_image(self, image_bytes: bytes):
        with self._lock:
            self._image = image_bytes
            self._created_at = time.time()
            self._code = None
            self._event.clear()

    def wait_for_code(self, timeout: float = 300) -> str | None:
        if self._event.wait(timeout):
            with self._lock:
                code = self._code
                self._code = None
                self._event.clear()
                return code
        return None

    def submit_code(self, code: str):
        if not code:
            return False
        with self._lock:
            self._code = code.strip()
            self._event.set()
        return True

    def get_status(self):
        with self._lock:
            return {
                "has_image": self._image is not None,
                "awaiting_code": not self._event.is_set(),
                "timestamp": self._created_at,
            }

    def get_image(self) -> bytes | None:
        with self._lock:
            return self._image

    def create_blueprint(self, prefix: str = "captcha") -> Blueprint:
        bp = Blueprint(prefix, __name__)

        @bp.route("/")
        def page():
            return render_template("captcha.html")

        def _image_response():
            img = self.get_image()
            if not img:
                return Response(status=204)
            return Response(img, mimetype="image/png")

        @bp.route("/image")
        def image_route():
            return _image_response()

        @bp.route("/captcha-image")
        def legacy_image_route():
            return _image_response()

        @bp.route("/status")
        def status_route():
            return jsonify(self.get_status())

        @bp.route("/submit", methods=["POST"])
        def submit_route():
            data = request.get_json() or {}
            code = data.get("code", "").strip()
            if not code:
                return jsonify({"ok": False, "message": "缺少验证码"}), 400
            self.submit_code(code)
            return jsonify({"ok": True})

        @bp.route("/submit-captcha", methods=["POST"])
        def legacy_submit_route():
            return submit_route()

        return bp


def start_standalone_captcha_server(manager: CaptchaManager, host: str = "127.0.0.1", port: int = 5000):
    from flask import Flask

    app = Flask(__name__)
    captcha_bp = manager.create_blueprint(prefix="captcha")
    app.register_blueprint(captcha_bp, url_prefix="/")

    def run_app():
        app.run(host=host, port=port, debug=False, use_reloader=False)

    thread = threading.Thread(target=run_app, daemon=True)
    thread.start()
    return thread
