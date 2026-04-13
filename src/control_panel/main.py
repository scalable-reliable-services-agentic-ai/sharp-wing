import redis
import yaml
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__, template_folder="templates", static_folder="static")


def load_config():
    # This path is relative to the container's WORKDIR
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


config = load_config()
r = redis.Redis(
    host=config["redis"]["host"], port=config["redis"]["port"], decode_responses=True
)

tps_key = config["redis"]["generator_tps_key"]
invalid_perc_key = config["redis"]["generator_invalid_perc_key"]


@app.route("/")
def main():
    return render_template("main.html")


@app.route("/generator", methods=["GET", "POST"])
def generator_controls():
    if request.method == "POST":
        target_tps = request.form.get("tps", type=float)
        invalid_percentage = request.form.get("invalid_percentage", type=int)

        # Basic validation
        if target_tps > 0:
            r.set(tps_key, target_tps)
        if 0 <= invalid_percentage <= 100:
            r.set(invalid_perc_key, invalid_percentage)

        return redirect(url_for("generator_controls"))

    # For GET request, get current values to display
    current_tps = r.get(tps_key) or config["generator"]["default_tps"]
    current_invalid_perc = (
        r.get(invalid_perc_key) or config["generator"]["default_invalid_percentage"]
    )

    return render_template(
        "generator.html",
        current_tps=float(current_tps),
        current_invalid_perc=int(current_invalid_perc),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003)
