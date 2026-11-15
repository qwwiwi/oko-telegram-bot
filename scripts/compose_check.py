"""Static Compose isolation checks only; not a Docker build/start claim."""

import json
from pathlib import Path

# The API is published on loopback only; put a TLS reverse proxy in front for remote use.
API_PORT = "127.0.0.1:8610:8610"


def validate(config):
    services = config["services"]
    if set(services) != {"db", "api", "poller", "worker", "ops"}:
        raise ValueError("unexpected_services")
    if "telegram-bot-api" in json.dumps(config):
        raise ValueError("local_bot_api_sidecar_forbidden")
    if not services["ops"].get("profiles"):
        raise ValueError("operator_tools_need_a_profile")
    for name, service in services.items():
        if "extends" in service or service.get("privileged") or service.get("network_mode"):
            raise ValueError("standalone_unprivileged_services_required")
        if service.get("ports") and name != "api":
            raise ValueError("only_api_may_publish_ports")
    if services["api"]["ports"] != [API_PORT]:
        raise ValueError("loopback_api_port_required")
    if services["api"]["command"][-2:] != ["--port", API_PORT.rsplit(":", 1)[1]]:
        raise ValueError("api_port_mismatch")
    if not all(item.endswith(":ro") for item in services["api"]["volumes"]):
        raise ValueError("readonly_api_mounts_required")
    for name in ("api", "worker", "poller", "ops"):
        if not services[name].get("read_only") or services[name].get("user") != "10001:10001":
            raise ValueError("unprivileged_readonly_application_required")


if __name__ == "__main__":
    validate(json.loads(Path("compose.yaml").read_text()))
    print("PASS: static Compose isolation; container build/start remains a separate gate")
