#!/usr/bin/env python3
"""Browser-level standalone admin/public acceptance test.

The test runs Caster, an NTRIP source, Chromium and the browser assertions
inside one WSL Linux network namespace. It never contacts the soak cluster.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import base64
import http.client
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from websockets.sync.client import connect


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "target/release/rcaster"
CAPTURE = ROOT / "target/com3-rtcm3-capture.bin"
OUTPUT = ROOT / "tools/web-e2e/runs" / datetime.now().strftime("%Y%m%d-%H%M%S")
sys.path.insert(0, str(ROOT / "tools/near-e2e"))
import rtcm  # noqa: E402


def free_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def request(url: str, method: str = "GET") -> urllib.response.addinfourl:
    context = ssl._create_unverified_context()
    return urllib.request.urlopen(
        urllib.request.Request(url, method=method),
        timeout=3,
        context=context,
    )


def request_mtls(
    url: str,
    ca_certificate: Path,
    certificate: Path,
    private_key: Path,
    authorization: str | None = None,
) -> urllib.response.addinfourl:
    context = ssl.create_default_context(cafile=str(ca_certificate))
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(str(certificate), str(private_key))
    headers = {}
    if authorization:
        headers["Authorization"] = authorization
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=headers),
        timeout=5,
        context=context,
    )


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with request(url) as response:
                if response.status < 500:
                    return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {url}")


class Cdp:
    def __init__(self, websocket_url: str):
        self.websocket = connect(websocket_url, max_size=None)
        self.next_id = 1
        self.events: list[dict] = []
        self.send("Page.enable")
        self.send("Runtime.enable")
        self.send("Network.enable")
        self.send("Log.enable")

    def send(self, method: str, params: dict | None = None) -> dict:
        request_id = self.next_id
        self.next_id += 1
        self.websocket.send(
            json.dumps(
                {
                    "id": request_id,
                    "method": method,
                    "params": params or {},
                }
            )
        )
        while True:
            message = json.loads(self.websocket.recv(timeout=20))
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(json.dumps(message["error"]))
                return message.get("result", {})
            self.events.append(message)

    def evaluate(self, expression: str):
        result = self.send(
            "Runtime.evaluate",
            {
                "expression": expression,
                "awaitPromise": True,
                "returnByValue": True,
                "userGesture": True,
            },
        )
        if "exceptionDetails" in result:
            details = result["exceptionDetails"]
            raise RuntimeError(
                details.get("exception", {}).get("description", details.get("text"))
            )
        return result.get("result", {}).get("value")

    def wait_for(self, expression: str, timeout: float = 20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.evaluate(f"(async()=>Boolean(await ({expression})))()"):
                return
            time.sleep(0.1)
        raise RuntimeError(f"timed out waiting for {expression}")

    def navigate(self, url: str) -> None:
        self.send("Page.navigate", {"url": url})
        self.wait_for("document.readyState === 'complete'")

    def close(self) -> None:
        self.websocket.close()


def new_page(debug_port: int, url: str) -> Cdp:
    with request(
        f"http://127.0.0.1:{debug_port}/json/new?about%3Ablank", method="PUT"
    ) as response:
        target = json.loads(response.read())
    page = Cdp(target["webSocketDebuggerUrl"])
    page.navigate(url)
    return page


def browser_problems(page: Cdp) -> tuple[list[str], list[str]]:
    console_errors: list[str] = []
    network_errors: list[str] = []
    for event in page.events:
        method = event.get("method")
        params = event.get("params", {})
        if method == "Runtime.exceptionThrown":
            console_errors.append(params.get("exceptionDetails", {}).get("text", "exception"))
        elif method == "Runtime.consoleAPICalled" and params.get("type") == "error":
            message = " ".join(
                str(argument.get("value", argument.get("description", "")))
                for argument in params.get("args", [])
            )
            if not message.startswith("Failed to load resource:"):
                console_errors.append(message)
        elif method == "Log.entryAdded" and params.get("entry", {}).get("level") == "error":
            entry = params["entry"]
            if not expected_resource_error(entry.get("url", ""), 0):
                console_errors.append(entry.get("text", "log error"))
        elif method == "Network.loadingFailed" and not params.get("canceled"):
            network_errors.append(params.get("errorText", "network failure"))
        elif method == "Network.responseReceived":
            response = params.get("response", {})
            if response.get("status", 0) >= 400:
                url = response.get("url", "")
                if not expected_resource_error(url, response["status"]):
                    network_errors.append(f"{response['status']} {url}")
    return console_errors, network_errors


def exercise_admin(page: Cdp, clustered: bool) -> dict:
    page.wait_for("document.querySelector('#username')")
    page.evaluate(
        "(() => {"
        "document.getElementById('username').value='browser-admin';"
        "document.getElementById('password').value='admin-secret';"
        "return login();"
        "})()"
    )
    page.wait_for("!document.getElementById('account').hidden")
    checks = {
        "admin_login": page.evaluate(
            "document.getElementById('account-name').textContent"
        )
    }
    page.evaluate("loadSources()")
    page.wait_for("document.getElementById('sources').textContent.includes('C63')")
    checks["admin_source_filter"] = page.evaluate(
        "document.getElementById('sources').textContent"
    )
    page.evaluate("showSourceHealth('C63')")
    page.wait_for(
        "document.getElementById('source-health')"
        ".textContent.includes('\"packets\"')"
    )
    checks["source_health"] = page.evaluate(
        "(() => {"
        "const value=JSON.parse(document.getElementById('source-health').textContent);"
        "return {health:value.health,position_available:"
        "value.profile.position_available,valid_frames:value.profile.valid_frames,"
        "message_numbers:value.profile.message_numbers};"
        "})()"
    )
    page.evaluate("showSourceHistory('C63')")
    page.wait_for(
        "document.getElementById('source-health')"
        ".textContent.includes('\"points\"')"
    )
    page.evaluate("loadOperationalViews()")
    if clustered:
        page.wait_for(
            "document.getElementById('config-status')"
            ".textContent.includes('\"generation\"')"
            " && document.getElementById('config-json').value.length > 25"
        )
        operation_result = "NOT_FOUND"
    else:
        page.wait_for(
            "document.getElementById('config-status')"
            ".textContent.includes('\"available\"')"
            " && document.querySelector('#config-revisions')"
            ".textContent.includes('Config history unavailable')"
        )
        operation_result = "UNAVAILABLE"
    page.evaluate(
        "(() => {"
        "document.getElementById('operation-id').value='missing';"
        "return loadOperation();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('operation')"
        f".textContent.includes('{operation_result}')"
    )
    checks["admin_health_history_config_operation"] = True
    if clustered:
        page.evaluate(
            "(() => {"
            "const config=JSON.parse(document.getElementById('config-json').value);"
            "config.source_health.alert_clear_s=31;"
            "document.getElementById('config-json').value=JSON.stringify(config,null,2);"
            "return saveConfig();"
            "})()"
        )
        page.wait_for(
            "(() => {const status=JSON.parse("
            "document.getElementById('config-status').textContent);"
            "return status.generation>=2;})()"
        )
        checks["config_apply"] = page.evaluate(
            "JSON.parse(document.getElementById('config-status').textContent).generation"
        )
    page.evaluate("setMaintenance(true)")
    page.wait_for(
        "document.getElementById('maintenance').textContent.includes('true')"
    )
    checks["maintenance_enable"] = page.evaluate(
        "document.getElementById('maintenance').textContent"
    )
    page.evaluate("setMaintenance(false)")
    page.wait_for(
        "document.getElementById('maintenance').textContent.includes('false')"
    )
    checks["maintenance_disable"] = page.evaluate(
        "document.getElementById('maintenance').textContent"
    )
    return checks


def exercise_admin_mutations(
    page: Cdp,
    clustered: bool,
    clear_port: int,
    clients: list[LocalClient],
    source: LocalSource,
) -> dict:
    page.evaluate("window.confirm=()=>true; true")
    checks: dict = {}

    page.evaluate(
        "(() => {"
        "userRevision=null;"
        "document.getElementById('user-id').value='browser-user';"
        "document.getElementById('user-name').value='browser-user';"
        "document.getElementById('user-password').value='user-secret';"
        "document.getElementById('user-role').value='user';"
        "document.getElementById('user-enabled').checked=true;"
        "document.getElementById('user-ntrip-enabled').checked=true;"
        "document.getElementById('user-web-enabled').checked=false;"
        "return saveUser();"
        "})()"
    )
    checks["user_create_status"] = page.evaluate(
        "document.getElementById('status').textContent"
    )
    if checks["user_create_status"].startswith("Error:"):
        raise RuntimeError(f"user create failed: {checks['user_create_status']}")
    page.wait_for(
        "document.getElementById('users').textContent.includes('browser-user')"
    )
    checks["user_scope_flags"] = page.evaluate(
        "fetch('/admin/v1/users/browser-user')"
        ".then(response=>response.json())"
        ".then(result=>({ntrip_enabled:result.user.ntrip_enabled,"
        "web_enabled:result.user.web_enabled}))"
    )
    if checks["user_scope_flags"] != {
        "ntrip_enabled": True,
        "web_enabled": False,
    }:
        raise RuntimeError(f"user scope flags failed: {checks['user_scope_flags']}")
    checks["user_create"] = True

    if clustered:
        page.wait_for(
            "JSON.parse(document.getElementById('cluster').textContent).ready === true",
            timeout=10,
        )
    user_client = connect_local_client(
        clear_port,
        "browser-user",
        "user-secret",
    )
    clients.append(user_client)
    page.evaluate("load()")
    if clustered:
        time.sleep(0.25)
        checks["sessions_after_client"] = page.evaluate(
            "fetch('/admin/v1/sessions').then(response=>response.json())"
        )
        page.wait_for(
            "document.getElementById('sessions')"
            ".textContent.includes('browser-user')"
        )

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#users tr')]"
        ".find(row=>row.textContent.includes('browser-user'));"
        "row.querySelectorAll('button')[0].click();"
        "document.getElementById('user-enabled').checked=false;"
        "return saveUser();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('users').textContent"
        ".includes('browser-useruserfalse')"
    )
    if not user_client.wait_closed():
        raise RuntimeError("disabling user did not disconnect the NTRIP client")
    checks["user_disable_kicks_client"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#users tr')]"
        ".find(row=>row.textContent.includes('browser-user'));"
        "row.querySelectorAll('button')[0].click();"
        "document.getElementById('user-enabled').checked=true;"
        "document.getElementById('user-web-enabled').checked=true;"
        "return saveUser();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('users').textContent"
        ".includes('browser-userusertrue')"
    )
    checks["user_scope_flags_update"] = page.evaluate(
        "fetch('/admin/v1/users/browser-user')"
        ".then(response=>response.json())"
        ".then(result=>({ntrip_enabled:result.user.ntrip_enabled,"
        "web_enabled:result.user.web_enabled}))"
    )
    if checks["user_scope_flags_update"] != {
        "ntrip_enabled": True,
        "web_enabled": True,
    }:
        raise RuntimeError(
            f"user scope flags update failed: "
            f"{checks['user_scope_flags_update']}"
        )

    page.evaluate(
        "(() => {"
        "mountRevision=null;"
        "document.getElementById('mount-name').value='TMP1';"
        "document.getElementById('mount-owner').value='browser-user';"
        "document.getElementById('mount-password').value='upload-secret';"
        "document.getElementById('mount-region').value='e2e';"
        "document.getElementById('mount-latitude').value='31.2';"
        "document.getElementById('mount-longitude').value='121.4';"
        "document.getElementById('mount-radius').value='25000';"
        "document.getElementById('mount-str-fields').value="
        "JSON.stringify({identifier:'Browser Farm',network:'RCaster-E2E',"
        "country:'CHN',carrier:'2',navigation_system:'GPS+GLO'});"
        "document.getElementById('mount-source-expectation').value="
        "JSON.stringify({formats:['rtcm3'],msm_versions:[4]});"
        "document.getElementById('mount-enabled').checked=true;"
        "document.getElementById('mount-advertise').checked=true;"
        "return saveMount();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('mountpoints').textContent.includes('TMP1')"
    )
    checks["mountpoint_metadata_create"] = page.evaluate(
        "fetch('/admin/v1/mountpoints/TMP1')"
        ".then(response=>response.json())"
        ".then(result=>({latitude:result.mountpoint.latitude,"
        "longitude:result.mountpoint.longitude,"
        "coverage_radius_m:result.mountpoint.coverage_radius_m,"
        "identifier:result.mountpoint.str_fields.identifier,"
        "network:result.mountpoint.str_fields.network,"
        "format:result.mountpoint.source_expectation.formats[0],"
        "msm_version:result.mountpoint.source_expectation.msm_versions[0]}))"
    )
    if checks["mountpoint_metadata_create"] != {
        "latitude": 31.2,
        "longitude": 121.4,
        "coverage_radius_m": 25000.0,
        "identifier": "Browser Farm",
        "network": "RCaster-E2E",
        "format": "rtcm3",
        "msm_version": 4,
    }:
        raise RuntimeError(
            f"mountpoint metadata create failed: "
            f"{checks['mountpoint_metadata_create']}"
        )
    checks["mountpoint_create"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#mountpoints tr')]"
        ".find(row=>row.textContent.includes('TMP1'));"
        "row.querySelectorAll('button')[0].click();"
        "document.getElementById('mount-region').value='e2e-edited';"
        "document.getElementById('mount-latitude').value='31.3';"
        "document.getElementById('mount-longitude').value='121.5';"
        "document.getElementById('mount-radius').value='30000';"
        "document.getElementById('mount-str-fields').value="
        "JSON.stringify({identifier:'Browser Farm Edited',network:'RCaster-E2E',"
        "country:'CHN',carrier:'1',navigation_system:'GPS+GLO'});"
        "document.getElementById('mount-source-expectation').value="
        "JSON.stringify({formats:['rtcm3'],msm_versions:[4,7]});"
        "document.getElementById('mount-enabled').checked=false;"
        "return saveMount();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('mountpoints').textContent.includes('TMP1browser-userfalse')"
    )
    checks["mountpoint_metadata_update"] = page.evaluate(
        "fetch('/admin/v1/mountpoints/TMP1')"
        ".then(response=>response.json())"
        ".then(result=>({latitude:result.mountpoint.latitude,"
        "longitude:result.mountpoint.longitude,"
        "coverage_radius_m:result.mountpoint.coverage_radius_m,"
        "identifier:result.mountpoint.str_fields.identifier,"
        "msm_versions:result.mountpoint.source_expectation.msm_versions}))"
    )
    if checks["mountpoint_metadata_update"] != {
        "latitude": 31.3,
        "longitude": 121.5,
        "coverage_radius_m": 30000.0,
        "identifier": "Browser Farm Edited",
        "msm_versions": [4, 7],
    }:
        raise RuntimeError(
            f"mountpoint metadata update failed: "
            f"{checks['mountpoint_metadata_update']}"
        )
    checks["mountpoint_update_disable"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#mountpoints tr')]"
        ".find(row=>row.textContent.includes('TMP1'));"
        "row.querySelectorAll('button')[1].click();"
        "return true;"
        "})()"
    )
    page.wait_for(
        "!document.getElementById('mountpoints').textContent.includes('TMP1')"
    )
    checks["mountpoint_delete"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#users tr')]"
        ".find(row=>row.textContent.includes('browser-user'));"
        "row.querySelectorAll('button')[1].click();"
        "return true;"
        "})()"
    )
    page.wait_for(
        "!document.getElementById('users').textContent.includes('browser-user')"
    )
    checks["user_delete"] = True

    page.evaluate(
        "(() => {"
        "blacklistRevision=null;"
        "document.getElementById('bl-id').value='e2e-rule';"
        "document.getElementById('bl-cidr').value='198.51.100.0/24';"
        "document.getElementById('bl-quota').value='0';"
        "document.getElementById('bl-description').value='browser e2e';"
        "return addBlacklist();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('blacklist').textContent.includes('e2e-rule')"
    )
    checks["blacklist_create"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#blacklist tr')]"
        ".find(row=>row.textContent.includes('e2e-rule'));"
        "row.querySelectorAll('button')[0].click();"
        "document.getElementById('bl-quota').value='1';"
        "return addBlacklist();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('blacklist').textContent"
        ".includes('e2e-rule198.51.100.0/241true')"
    )
    checks["blacklist_update"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#blacklist tr')]"
        ".find(row=>row.textContent.includes('e2e-rule'));"
        "row.querySelectorAll('button')[1].click();"
        "return true;"
        "})()"
    )
    page.wait_for(
        "!document.getElementById('blacklist').textContent.includes('e2e-rule')"
    )
    checks["blacklist_delete"] = True

    page.evaluate(
        "(() => {"
        "document.getElementById('api-token-name').value='browser-token';"
        "document.getElementById('api-token-scope').value='read';"
        "document.getElementById('api-token-expiry').value='60';"
        "return createApiToken();"
        "})()"
    )
    page.wait_for(
        "document.getElementById('api-token-secret')"
        ".textContent.startsWith('rcaster_')"
    )
    page.wait_for(
        "document.getElementById('api-tokens').textContent.includes('browser-token')"
    )
    checks["api_token_create"] = page.evaluate(
        "({secret_prefix:document.getElementById('api-token-secret')"
        ".textContent.slice(0,8),"
        "listed:document.getElementById('api-tokens')"
        ".textContent.includes('browser-token')})"
    )
    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#api-tokens tr')]"
        ".find(row=>row.textContent.includes('browser-token'));"
        "row.querySelector('button').click();"
        "return true;"
        "})()"
    )
    page.wait_for(
        "!document.getElementById('api-tokens').textContent.includes('browser-token')"
    )
    checks["api_token_revoke"] = True

    if clustered:
        page.evaluate(
            "(() => {"
            "userRevision=null;"
            "document.getElementById('user-id').value='batch-user';"
            "document.getElementById('user-name').value='batch-user';"
            "document.getElementById('user-password').value='batch-secret';"
            "document.getElementById('user-role').value='user';"
            "document.getElementById('user-enabled').checked=true;"
            "return saveUser();"
            "})()"
        )
        page.wait_for(
            "document.getElementById('users').textContent.includes('batch-user')"
        )
        page.evaluate(
            "(() => {"
            "document.getElementById('batch-action').value='users.disable';"
            "document.getElementById('batch-targets').value='batch-user';"
            "return previewBatchDisable();"
            "})()"
        )
        page.wait_for(
            "!document.getElementById('batch-disable-start').hidden"
        )
        page.evaluate("startBatchDisable()")
        page.wait_for(
            "document.getElementById('operation-id').value.length > 0"
        )
        page.wait_for(
            "(() => {"
            "const id=document.getElementById('operation-id').value;"
            "return api('/admin/v1/operations/'+encodeURIComponent(id))"
            ".then(operation=>operation.status==='SUCCEEDED');"
            "})()",
            timeout=20,
        )
        page.evaluate("load()")
        page.wait_for(
            "document.getElementById('users').textContent"
            ".includes('batch-useruserfalse')"
        )
        checks["batch_users_disable"] = page.evaluate(
            "document.getElementById('operation-id').value"
        )
        page.evaluate(
            "(() => {"
            "const row=[...document.querySelectorAll('#users tr')]"
            ".find(row=>row.textContent.includes('batch-user'));"
            "row.querySelectorAll('button')[1].click();"
            "return true;"
            "})()"
        )
        page.wait_for(
            "!document.getElementById('users').textContent.includes('batch-user')"
        )

    owner_client = connect_local_client(clear_port, "owner", "secret")
    clients.append(owner_client)
    page.evaluate("load()")
    page.wait_for(
        "(() => {"
        "const row=[...document.querySelectorAll('#sessions tr')]"
        ".find(row=>row.textContent.includes('client')"
        "&&row.textContent.includes('owner'));"
        "return Boolean(row);"
        "})()"
    )
    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#sessions tr')]"
        ".find(row=>row.textContent.includes('client')"
        "&&row.textContent.includes('owner'));"
        "row.querySelector('button').click();"
        "return true;"
        "})()"
    )
    if not owner_client.wait_closed():
        raise RuntimeError("session disconnect did not close the NTRIP client")
    page.evaluate("load()")
    page.wait_for(
        "![...document.querySelectorAll('#sessions tr')]"
        ".some(row=>row.textContent.includes('client')"
        "&&row.textContent.includes('owner'))"
    )
    checks["session_disconnect_kicks_client"] = True

    page.evaluate(
        "(() => {"
        "const row=[...document.querySelectorAll('#sources tr')]"
        ".find(row=>row.textContent.includes('C63'));"
        "row.querySelectorAll('button')[3].click();"
        "return true;"
        "})()"
    )
    if not source.closed.wait(5):
        raise RuntimeError("source disconnect did not close the NTRIP source")
    page.evaluate("loadSources()")
    page.wait_for(
        "!document.getElementById('sources').textContent.includes('C63')"
    )
    checks["source_disconnect_kicks_source"] = True
    page.evaluate("logout()")
    page.wait_for(
        "!document.getElementById('login').hidden"
        " && document.getElementById('account').hidden"
    )
    checks["admin_logout"] = page.evaluate(
        "document.getElementById('status').textContent"
    )
    return checks


def expected_resource_error(url: str, status: int) -> bool:
    return (
        url.endswith("/favicon.ico")
        or url.endswith("/admin/v1/auth/session") and status in (0, 401)
        or url.endswith("/admin/v1/config/revisions?limit=20")
        and status in (0, 503)
        or url.endswith("/admin/v1/operations/missing")
    )


class LocalSource:
    def __init__(self, port: int, payload: bytes):
        self.payload = payload
        self.stop = threading.Event()
        self.closed = threading.Event()
        self.error: str | None = None
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.socket.sendall(
            b"SOURCE upload /C63\r\n"
            b"Source-Agent: RCaster-browser-e2e/1.0\r\n\r\n"
        )
        header = bytearray()
        while b"\r\n\r\n" not in header:
            chunk = self.socket.recv(1024)
            if not chunk:
                break
            header.extend(chunk)
        if not bytes(header).startswith(b"ICY 200 OK"):
            raise RuntimeError(f"source rejected: {bytes(header)!r}")
        self.thread = threading.Thread(target=self.run, name="browser-e2e-source")
        self.thread.start()

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                self.socket.sendall(self.payload)
                self.stop.wait(0.5)
        except OSError:
            self.error = "source socket closed"
        finally:
            self.closed.set()

    def close(self) -> None:
        self.stop.set()
        self.socket.close()
        self.thread.join(timeout=3)


class LocalClient:
    def __init__(self, port: int, username: str, password: str):
        self.closed = threading.Event()
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=5)
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.socket.sendall(
            (
                "GET /C63 HTTP/1.1\r\n"
                "Host: 127.0.0.1\r\n"
                "Ntrip-Version: Ntrip/2.0\r\n"
                f"Authorization: Basic {credentials}\r\n\r\n"
            ).encode()
        )
        header = bytearray()
        while b"\r\n\r\n" not in header:
            chunk = self.socket.recv(1024)
            if not chunk:
                break
            header.extend(chunk)
        if b" 200 " not in header.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"client rejected: {bytes(header)!r}")
        self.thread = threading.Thread(target=self.run, name="browser-e2e-client")
        self.thread.start()

    def run(self) -> None:
        try:
            while not self.closed.is_set():
                if not self.socket.recv(4096):
                    break
        except OSError:
            pass
        finally:
            self.closed.set()

    def wait_closed(self, timeout: float = 5.0) -> bool:
        return self.closed.wait(timeout)

    def close(self) -> None:
        self.closed.set()
        self.socket.close()
        self.thread.join(timeout=3)


def connect_local_client(
    port: int,
    username: str,
    password: str,
    timeout: float = 10.0,
) -> LocalClient:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return LocalClient(port, username, password)
        except (OSError, RuntimeError) as error:
            last_error = error
            time.sleep(0.1)
    raise RuntimeError(f"NTRIP client session could not be established: {last_error}")


class AdminProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, upstream_port: int, client_context: ssl.SSLContext):
        super().__init__(address, AdminProxyHandler)
        self.upstream_port = upstream_port
        self.client_context = client_context

    def handle_error(self, request, client_address) -> None:
        if isinstance(sys.exception(), (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


class AdminProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:
        self.proxy()

    def do_POST(self) -> None:
        self.proxy()

    def do_PATCH(self) -> None:
        self.proxy()

    def do_DELETE(self) -> None:
        self.proxy()

    def proxy(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else None
        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in {"host", "connection", "content-length"}
        }
        headers["Host"] = f"127.0.0.1:{self.server.upstream_port}"
        connection = http.client.HTTPSConnection(
            "127.0.0.1",
            self.server.upstream_port,
            context=self.server.client_context,
            timeout=20,
        )
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read()
            self.send_response(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in {
                    "connection",
                    "content-length",
                    "transfer-encoding",
                }:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as error:
            data = json.dumps(
                {"code": "PROXY_ERROR", "message": str(error)}
            ).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        finally:
            connection.close()


def hash_password(password: str) -> str:
    result = subprocess.run(
        [str(BINARY), "hash-password"],
        input=f"{password}\n",
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def write_accounts(path: Path) -> None:
    now = "2026-11-25T00:00:00Z"
    accounts = {
        "users": [
            {
                "schema_version": 1,
                "mod_revision": 1,
                "user_id": "owner-id",
                "username": "owner",
                "password_hash": hash_password("secret"),
                "auth_version": 1,
                "enabled": True,
                "ntrip_enabled": True,
                "web_enabled": True,
                "role": "user",
                "created_at": now,
                "updated_at": now,
            },
            {
                "schema_version": 1,
                "mod_revision": 1,
                "user_id": "browser-admin",
                "username": "browser-admin",
                "password_hash": hash_password("admin-secret"),
                "auth_version": 1,
                "enabled": True,
                "ntrip_enabled": False,
                "web_enabled": True,
                "role": "admin",
                "created_at": now,
                "updated_at": now,
            },
        ],
        "mountpoints": [
            {
                "schema_version": 1,
                "mod_revision": 1,
                "mountpoint": "C63",
                "owner_user_id": "owner-id",
                "enabled": True,
                "ntrip1_upload_password_hash": hash_password("upload"),
                "advertise": True,
                "region": "browser-test",
                "latitude": 31.2,
                "longitude": 121.4,
                "coverage_radius_m": 25000.0,
                "created_at": now,
                "updated_at": now,
            }
        ],
    }
    path.write_text(json.dumps(accounts), encoding="utf-8")


def run_test(temp: Path, clustered: bool) -> dict:
    temp.mkdir(parents=True, exist_ok=True)
    ports = free_ports(9 if clustered else 5)
    clear_port, public_port, admin_port, health_port, debug_port = ports[:5]
    etcd_client_port, etcd_peer_port, tls_port, admin_proxy_port = (
        ports[5:] if clustered else (None, None, None, None)
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(temp / "server.key"),
            "-out",
            str(temp / "server.crt"),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    os.chmod(temp / "server.key", 0o600)
    if clustered:
        (temp / "client.ext").write_text(
            "basicConstraints=CA:FALSE\nextendedKeyUsage=clientAuth\n",
            encoding="utf-8",
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(temp / "client.key"),
                "-out",
                str(temp / "client.csr"),
                "-subj",
                "/CN=rcaster-browser-client",
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(temp / "client.csr"),
                "-CA",
                str(temp / "server.crt"),
                "-CAkey",
                str(temp / "server.key"),
                "-CAcreateserial",
                "-out",
                str(temp / "client.crt"),
                "-days",
                "1",
                "-extfile",
                str(temp / "client.ext"),
            ],
            check=True,
            capture_output=True,
        )
    write_accounts(temp / "accounts.json")
    subprocess.run(
        [
            str(BINARY),
            "import-accounts",
            "--database",
            str(temp / "accounts.redb"),
            "--input",
            str(temp / "accounts.json"),
        ],
        check=True,
        capture_output=True,
    )
    config = {
        "bind": f"127.0.0.1:{clear_port}",
        "public_bind": f"127.0.0.1:{public_port}",
        "admin_bind": f"127.0.0.1:{admin_port}",
        "health_bind": f"127.0.0.1:{health_port}",
        "account_db": "accounts.redb",
        "tls_certificate": "server.crt",
        "tls_private_key": "server.key",
        "client_auth_required": True,
        "client_auth_require_tls": False,
        "source_auth_require_tls": False,
        "head_timeout_ms": 5000,
    }
    if clustered:
        config["tls_bind"] = f"127.0.0.1:{tls_port}"
        config["cluster"] = {
            "etcd_endpoints": [f"http://127.0.0.1:{etcd_client_port}"],
            "cluster_id": "browser-e2e",
            "caster_id": "browser-caster",
            "runtime_epoch": f"epoch-{time.time_ns()}",
            "relay_endpoint": "127.0.0.1:1",
            "coordinator_tls": {
                "ca_certificate": "server.crt",
                "certificate": "server.crt",
                "private_key": "server.key",
                "server_name": "localhost",
                "service_token": "browser-e2e",
            },
        }
    (temp / "caster.json").write_text(json.dumps(config), encoding="utf-8")

    result: dict = {
        "mode": "clustered" if clustered else "standalone",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "checks": {},
        "console_errors": [],
        "network_errors": [],
    }
    etcd = None
    if clustered:
        (temp / "etcd-data").mkdir()
        etcd = subprocess.Popen(
            [
                "/usr/bin/etcd",
                "--name",
                "browser",
                "--data-dir",
                str(temp / "etcd-data"),
                "--listen-client-urls",
                f"http://127.0.0.1:{etcd_client_port}",
                "--advertise-client-urls",
                f"http://127.0.0.1:{etcd_client_port}",
                "--listen-peer-urls",
                f"http://127.0.0.1:{etcd_peer_port}",
                "--initial-advertise-peer-urls",
                f"http://127.0.0.1:{etcd_peer_port}",
                "--initial-cluster",
                f"browser=http://127.0.0.1:{etcd_peer_port}",
                "--initial-cluster-token",
                f"browser-{time.time_ns()}",
                "--initial-cluster-state",
                "new",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        wait_http(f"http://127.0.0.1:{etcd_client_port}/health")
        subprocess.run(
            [
                "/usr/bin/etcdctl",
                "--endpoints",
                f"http://127.0.0.1:{etcd_client_port}",
                "put",
                "/rcaster/browser-e2e/config/caster",
                '{"schema_version":1,"generation":1}',
            ],
            check=True,
            capture_output=True,
        )
    caster_log = (temp / "caster.log").open("w+", encoding="utf-8")
    caster = subprocess.Popen(
        [str(BINARY), "serve", "--config", str(temp / "caster.json")],
        cwd=temp,
        stdout=caster_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    source = None
    chrome = None
    admin = None
    public = None
    admin_proxy = None
    proxy_thread = None
    clients: list[LocalClient] = []
    try:
        wait_http(f"http://127.0.0.1:{health_port}/health/live")
        wait_http(f"https://127.0.0.1:{public_port}/")
        source_payload = rtcm.payload_for_station(
            rtcm.first_epoch(CAPTURE.read_bytes()),
            63,
            31.2,
            121.4,
        )
        source = LocalSource(clear_port, source_payload)

        profile = temp / "chrome"
        profile.mkdir()
        chrome_home = temp / "home"
        chrome_home.mkdir()
        chrome = subprocess.Popen(
            [
                shutil.which("chromium") or "/usr/bin/chromium",
                "--headless=new",
                "--disable-gpu",
                "--disable-background-networking",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-sync",
                "--ignore-certificate-errors",
                "--no-first-run",
                "--no-sandbox",
                "--password-store=basic",
                f"--remote-debugging-port={debug_port}",
                f"--user-data-dir={profile}",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env={**os.environ, "HOME": str(chrome_home)},
        )
        wait_http(f"http://127.0.0.1:{debug_port}/json/version")
        if clustered:
            client_context = ssl.create_default_context(
                cafile=str(temp / "server.crt")
            )
            client_context.check_hostname = False
            client_context.load_cert_chain(
                str(temp / "client.crt"),
                str(temp / "client.key"),
            )
            admin_proxy = AdminProxyServer(
                ("127.0.0.1", admin_proxy_port),
                admin_port,
                client_context,
            )
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(
                str(temp / "server.crt"),
                str(temp / "server.key"),
            )
            admin_proxy.socket = server_context.wrap_socket(
                admin_proxy.socket,
                server_side=True,
            )
            proxy_thread = threading.Thread(
                target=admin_proxy.serve_forever,
                name="browser-e2e-admin-proxy",
                daemon=True,
            )
            proxy_thread.start()

        if clustered:
            with request_mtls(
                f"https://127.0.0.1:{admin_port}/admin/",
                temp / "server.crt",
                temp / "client.crt",
                temp / "client.key",
            ) as response:
                admin_page = response.read().decode("utf-8")
            assert "RCaster Admin" in admin_page
            with request_mtls(
                f"https://127.0.0.1:{admin_port}/admin/v1/config/status",
                temp / "server.crt",
                temp / "client.crt",
                temp / "client.key",
                "Bearer browser-e2e",
            ) as response:
                config_status = json.loads(response.read())
            assert config_status["available"] is True
            assert config_status["generation"] == 1
            result["checks"]["cluster_mtls_admin_config"] = config_status
        admin_url_port = admin_proxy_port if clustered else admin_port
        admin = new_page(debug_port, f"https://127.0.0.1:{admin_url_port}/admin/")
        result["checks"].update(exercise_admin(admin, clustered))
        if source.error:
            result["checks"]["source_error"] = source.error

        public = new_page(debug_port, f"https://127.0.0.1:{public_port}/")
        public.wait_for(
            "document.getElementById('mounts').textContent.includes('C63')"
        )
        result["checks"]["public_map_bbox"] = public.evaluate("mapBbox()")
        public.evaluate("refreshMap()")
        result["checks"]["public_map_data"] = public.evaluate(
            "({features:(mapData?.features||[]).length,"
            "sample:(mapData?.features||[])[0]?.properties||null})"
        )
        public.wait_for("document.querySelectorAll('#map circle').length > 0")
        result["checks"]["public_directory_map"] = public.evaluate(
            "({mounts:document.getElementById('mounts').textContent,"
            "map_points:document.querySelectorAll('#map circle').length})"
        )
        result["checks"]["public_login_result"] = public.evaluate(
            "(() => {"
            "document.getElementById('username').value='owner';"
            "document.getElementById('password').value='secret';"
            "return login().then(() => ({"
            "status:document.getElementById('status').textContent,csrf:Boolean(csrf)"
            "}));"
            "})()"
        )
        public.wait_for(
            "document.getElementById('status')"
            ".textContent.includes('Signed in as owner')"
        )
        public.evaluate("document.querySelector('#mounts button').click(); true")
        public.wait_for(
            "document.getElementById('monitor')"
            ".textContent.includes('monitor_snapshot')",
            timeout=25,
        )
        result["checks"]["public_login_ticket_websocket"] = public.evaluate(
            "(() => {const value=JSON.parse("
            "document.getElementById('monitor').textContent);"
            "return {type:value.type,mountpoint:value.mountpoint,"
            "satellite_count:value.snapshot.profile.satellite_count,"
            "msm_observation_count:value.snapshot.profile.msm_observation_count};"
            "})()"
        )
        result["checks"].update(
            exercise_admin_mutations(
                admin,
                clustered,
                clear_port,
                clients,
                source,
            )
        )
        with request(f"http://127.0.0.1:{health_port}/metrics") as response:
            metrics_text = response.read().decode("utf-8")
        required_metrics = [
            "rcaster_parser_framing_errors_total",
            "rcaster_parser_resynchronizations_total",
            "rcaster_parser_dropped_bytes_total",
            "rcaster_parser_max_pending_bytes",
            "rcaster_relay_slow_consumers_total",
            "rcaster_etcd_admission_latency_seconds",
            "rcaster_etcd_lease_remaining_seconds",
            "rcaster_etcd_watch_lag_revisions",
            "rcaster_etcd_watch_recoveries_total",
            "rcaster_near_query_latency_seconds",
        ]
        missing_metrics = [
            name for name in required_metrics if name not in metrics_text
        ]
        if missing_metrics:
            raise RuntimeError(f"missing runtime metrics: {missing_metrics}")
        result["checks"]["runtime_metrics"] = required_metrics

        for page in (admin, public):
            if page is None:
                continue
            console_errors, network_errors = browser_problems(page)
            result["console_errors"].extend(console_errors)
            result["network_errors"].extend(network_errors)
        if result["console_errors"] or result["network_errors"]:
            raise RuntimeError(
                "browser errors: "
                + json.dumps(
                    {
                        "console": result["console_errors"],
                        "network": result["network_errors"],
                    }
                )
            )
        result["pass"] = True
    except Exception as error:
        result["pass"] = False
        result["error"] = f"{type(error).__name__}: {error}"
        result["traceback"] = traceback.format_exc()
        for name, page in (("admin", admin), ("public", public)):
            if page is not None:
                try:
                    result[f"{name}_state"] = page.evaluate(
                        "({url:location.href,title:document.title,"
                        "body:document.body?.innerText.slice(0,1000)||''})"
                    )
                except Exception:
                    pass
    finally:
        for page in (admin, public):
            if page is not None:
                page.close()
        if source is not None:
            source.close()
        for client in clients:
            client.close()
        if admin_proxy is not None:
            admin_proxy.shutdown()
            admin_proxy.server_close()
        if proxy_thread is not None:
            proxy_thread.join(timeout=3)
        for process in (chrome, caster, etcd):
            if process is not None and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=5)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        caster_log.flush()
        caster_log.seek(0)
        result["caster_log"] = caster_log.read()
        caster_log.close()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
    return result


def main() -> int:
    if not BINARY.exists():
        raise SystemExit(f"release binary missing: {BINARY}")
    if not CAPTURE.exists():
        raise SystemExit(f"capture missing: {CAPTURE}")
    if shutil.which("chromium") is None:
        raise SystemExit("chromium is required in WSL")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="rcaster-web-e2e."))
    results: dict[str, dict] = {}
    try:
        for mode, clustered in (("standalone", False), ("clustered", True)):
            try:
                results[mode] = run_test(temp / mode, clustered)
            except Exception as error:
                results[mode] = {
                    "mode": mode,
                    "pass": False,
                    "error": f"{type(error).__name__}: {error}",
                    "checks": {},
                    "console_errors": [],
                    "network_errors": [],
                }
    except Exception as error:
        results["harness"] = {"pass": False, "error": str(error)}
    finally:
        if temp.name.startswith("rcaster-web-e2e.") and temp.parent == Path("/tmp"):
            shutil.rmtree(temp, ignore_errors=True)
    result = {
        "pass": bool(results) and all(item.get("pass") for item in results.values()),
        "modes": results,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    (OUTPUT / "result.json").write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {"output": str(OUTPUT), **result},
            indent=2,
        )
    )
    return 0 if result.get("pass") else 1


if __name__ == "__main__":
    sys.exit(main())
