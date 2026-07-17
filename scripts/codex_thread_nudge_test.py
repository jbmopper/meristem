#!/usr/bin/env python3
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
import time
import tempfile
import textwrap
import types
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("codex-thread-nudge.py")
SPEC = importlib.util.spec_from_file_location("codex_thread_nudge", MODULE_PATH)
NUDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NUDGE)


FAKE_APP_SERVER = r'''#!/usr/bin/env python3
import json
import os
import sys
import time

scenario, record_path, reconcile_client_id = sys.argv[1:4]
methods = []

def send(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()

def record():
    sensitive = [
        key for key in (
            "MERISTEM_TOKEN", "MERISTEM_TOKEN_FILE", "MERISTEM_DATABASE_URL",
            "ROOT_TOKEN_FILE", "PGPASSWORD", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
            "TEST_SENTINEL_SECRET"
        ) if key in os.environ
    ]
    with open(record_path, "w", encoding="utf-8") as handle:
        json.dump({"methods": methods, "sensitive_env": sensitive}, handle)

for raw in sys.stdin:
    message = json.loads(raw)
    method = message.get("method")
    if method:
        methods.append(method)
    if method == "initialize":
        send({"id": message["id"], "result": {"secret": "RAW-SECRET-SENTINEL"}})
    elif method == "initialized":
        continue
    elif method == "thread/resume":
        thread_id = message["params"]["threadId"]
        turns = []
        status = {"type": "idle"}
        if scenario in ("active", "active_failure", "active_interrupted", "server_request"):
            status = {"type": "active", "activeFlags": []}
            turns = [{"id": "active-turn", "status": "inProgress", "items": []}]
        elif scenario == "waiting":
            status = {"type": "active", "activeFlags": ["waitingOnApproval"]}
            turns = [{"id": "active-turn", "status": "inProgress", "items": []}]
        elif scenario == "ambiguous_active":
            status = {"type": "active", "activeFlags": []}
            turns = [
                {"id": "turn-a", "status": "inProgress", "items": []},
                {"id": "turn-b", "status": "inProgress", "items": []},
            ]
        elif scenario == "stale_idle":
            turns = [{"id": "stale-turn", "status": "inProgress", "items": []}]
        elif scenario == "reconcile":
            turns = [{
                "id": "reconciled-turn",
                "status": "completed",
                "items": [{"type": "userMessage", "clientId": reconcile_client_id}],
            }]
        elif scenario == "reconcile_active":
            status = {"type": "active", "activeFlags": []}
            turns = [{
                "id": "reconciled-turn",
                "status": "inProgress",
                "items": [{"type": "userMessage", "clientId": reconcile_client_id}],
            }]
        send({
            "id": message["id"],
            "result": {
                "thread": {
                    "id": thread_id,
                    "status": status,
                    "turns": turns,
                    "secret": "HISTORY-SECRET-SENTINEL",
                }
            },
        })
        if scenario == "reconcile_active":
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": thread_id,
                    "turn": {
                        "id": "reconciled-turn",
                        "status": "completed",
                        "items": [],
                    },
                },
            })
    elif method in ("turn/start", "turn/steer"):
        thread_id = message["params"]["threadId"]
        turn_id = "active-turn" if method == "turn/steer" else "started-turn"
        if scenario == "server_request":
            send({"id": 9001, "method": "item/permissions/request", "params": {}})
            continue
        if scenario == "request_error":
            send({
                "id": message["id"],
                "error": {"code": -32000, "message": "RAW-SECRET-SENTINEL"},
            })
            continue
        if scenario == "partial_line":
            sys.stdout.write('{"id":')
            sys.stdout.flush()
            time.sleep(5)
            continue
        if scenario == "active_failure":
            status = "failed"
        elif scenario == "active_interrupted":
            status = "interrupted"
        else:
            status = "completed"
        # Exercise response correlation and buffering: an unrelated completion,
        # then the matching completion, both arrive before the response.
        send({
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {"id": "unrelated-turn", "status": "completed", "items": []},
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {"id": turn_id, "status": status, "items": []},
            },
        })
        if method == "turn/steer":
            send({"id": message["id"], "result": {"turnId": turn_id}})
        else:
            send({"id": message["id"], "result": {"turn": {"id": turn_id}}})
        record()
    else:
        record()
'''


class NudgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.batch = self.root / "delivery.tsv"
        self.batch.write_text("event\tactor\tkind\tsubject\n", encoding="utf-8")
        self.batch.chmod(0o600)
        self.marker = self.root / "delivery.json"
        self.fake = self.root / "fake-app-server.py"
        self.fake.write_text(textwrap.dedent(FAKE_APP_SERVER), encoding="utf-8")
        self.fake.chmod(0o700)
        self.record = self.root / "record.json"
        self.thread_id = "019f6309-db25-75c2-b87d-41d3050581db"
        self.args = types.SimpleNamespace(
            batch_file=str(self.batch),
            marker_file=str(self.marker),
            codex_bin=sys.executable,
            thread_id=self.thread_id,
            repo_root=str(self.root),
            request_timeout=2.0,
            completion_timeout=2.0,
            idle_only=False,
        )
        self.environment = {
            "HOME": str(self.root),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "MERISTEM_TOKEN": "secret",
            "MERISTEM_TOKEN_FILE": "/secret/token",
            "MERISTEM_DATABASE_URL": "postgres://secret",
            "ROOT_TOKEN_FILE": "/secret/root",
            "PGPASSWORD": "secret",
            "OPENAI_API_KEY": "secret",
            "ANTHROPIC_API_KEY": "secret",
            "TEST_SENTINEL_SECRET": "secret",
        }

    def tearDown(self):
        self.temp.cleanup()

    def command(self, scenario, reconcile_client_id="none"):
        return [
            sys.executable,
            str(self.fake),
            scenario,
            str(self.record),
            reconcile_client_id,
        ]

    def marker_value(self):
        return json.loads(self.marker.read_text(encoding="utf-8"))

    def test_idle_start_buffers_early_completion_and_sanitizes(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = NUDGE.deliver(
                self.args, self.command("idle"), environment=self.environment
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(self.marker_value()["state"], "completed")
        record = json.loads(self.record.read_text(encoding="utf-8"))
        self.assertIn("turn/start", record["methods"])
        self.assertNotIn("turn/steer", record["methods"])
        self.assertEqual(record["sensitive_env"], [])
        emitted = stdout.getvalue() + stderr.getvalue()
        self.assertNotIn("SECRET-SENTINEL", emitted)

    def test_active_turn_is_steered(self):
        result = NUDGE.deliver(
            self.args, self.command("active"), environment=self.environment
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        marker = self.marker_value()
        self.assertEqual(marker["mode"], "steer")
        self.assertEqual(marker["turn_id"], "active-turn")
        record = json.loads(self.record.read_text(encoding="utf-8"))
        self.assertIn("turn/steer", record["methods"])
        self.assertNotIn("turn/start", record["methods"])

    def test_idle_only_never_steers_an_active_turn(self):
        self.args.idle_only = True
        with self.assertRaises(NUDGE.TransportError):
            NUDGE.deliver(
                self.args, self.command("active"), environment=self.environment
            )
        self.assertFalse(self.marker.exists())

    def test_idle_status_ignores_stale_in_progress_history(self):
        result = NUDGE.deliver(
            self.args, self.command("stale_idle"), environment=self.environment
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(self.marker_value()["mode"], "start")

    def test_waiting_and_ambiguous_active_turns_do_not_dispatch(self):
        for scenario in ("waiting", "ambiguous_active"):
            with self.subTest(scenario=scenario):
                if self.marker.exists():
                    self.marker.unlink()
                with self.assertRaises(NUDGE.TransportError):
                    NUDGE.deliver(
                        self.args, self.command(scenario), environment=self.environment
                    )
                self.assertFalse(self.marker.exists())

    def test_dispatching_marker_reconciles_positive_history_without_resubmit(self):
        batch_id, _ = NUDGE.batch_identity(self.batch)
        client_id = NUDGE.client_message_id(batch_id)
        NUDGE.atomic_write_json(
            self.marker,
            NUDGE.marker_for(
                batch_id,
                self.thread_id,
                client_id,
                "dispatching",
                mode="start",
                expected_turn_id=None,
            ),
        )
        result = NUDGE.deliver(
            self.args,
            self.command("reconcile", client_id),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(self.marker_value()["state"], "completed")
        # The fake exits when the client closes after resume, so no admission
        # record is created; this confirms start/steer was not sent.
        self.assertFalse(self.record.exists())

    def test_dispatching_without_positive_history_is_ambiguous(self):
        batch_id, _ = NUDGE.batch_identity(self.batch)
        client_id = NUDGE.client_message_id(batch_id)
        NUDGE.atomic_write_json(
            self.marker,
            NUDGE.marker_for(
                batch_id,
                self.thread_id,
                client_id,
                "dispatching",
                mode="start",
                expected_turn_id=None,
            ),
        )
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.deliver(
                self.args, self.command("idle"), environment=self.environment
            )
        self.assertEqual(self.marker_value()["state"], "dispatching")

    def test_dispatching_marker_reconciles_running_turn_then_completion(self):
        batch_id, _ = NUDGE.batch_identity(self.batch)
        client_id = NUDGE.client_message_id(batch_id)
        NUDGE.atomic_write_json(
            self.marker,
            NUDGE.marker_for(
                batch_id,
                self.thread_id,
                client_id,
                "dispatching",
                mode="start",
                expected_turn_id=None,
            ),
        )
        result = NUDGE.deliver(
            self.args,
            self.command("reconcile_active", client_id),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        marker = self.marker_value()
        self.assertEqual(marker["state"], "completed")
        self.assertEqual(marker["turn_id"], "reconciled-turn")

    def test_accepted_marker_without_history_is_never_resubmitted(self):
        batch_id, _ = NUDGE.batch_identity(self.batch)
        client_id = NUDGE.client_message_id(batch_id)
        NUDGE.atomic_write_json(
            self.marker,
            NUDGE.marker_for(
                batch_id,
                self.thread_id,
                client_id,
                "accepted",
                mode="start",
                turn_id="lost-turn",
            ),
        )
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.deliver(
                self.args, self.command("idle"), environment=self.environment
            )
        self.assertEqual(self.marker_value()["state"], "accepted")

    def test_server_request_fails_closed_after_dispatch(self):
        with self.assertRaises(NUDGE.UnsafeServerRequest):
            NUDGE.deliver(
                self.args,
                self.command("server_request"),
                environment=self.environment,
            )
        self.assertEqual(self.marker_value()["state"], "dispatching")

    def test_failed_completion_is_terminal_and_not_replayable(self):
        result = NUDGE.deliver(
            self.args,
            self.command("active_failure"),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
        marker = self.marker_value()
        self.assertEqual(marker["state"], "terminal_failure")
        self.assertEqual(marker["turn_status"], "failed")

    def test_interrupted_completion_is_terminal_and_not_replayable(self):
        result = NUDGE.deliver(
            self.args,
            self.command("active_interrupted"),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
        marker = self.marker_value()
        self.assertEqual(marker["state"], "terminal_failure")
        self.assertEqual(marker["turn_status"], "interrupted")

    def test_generic_admission_error_remains_dispatching(self):
        with self.assertRaises(NUDGE.RequestRejected):
            NUDGE.deliver(
                self.args, self.command("request_error"), environment=self.environment
            )
        self.assertEqual(self.marker_value()["state"], "dispatching")

    def test_partial_json_line_obeys_request_deadline(self):
        self.args.request_timeout = 0.2
        started = time.monotonic()
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.deliver(
                self.args, self.command("partial_line"), environment=self.environment
            )
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(self.marker_value()["state"], "dispatching")

    def test_marker_and_batch_permissions_fail_closed(self):
        self.batch.chmod(0o644)
        with self.assertRaises(NUDGE.ConfigError):
            NUDGE.batch_identity(self.batch)
        self.batch.chmod(0o600)
        NUDGE.atomic_write_json(self.marker, {"state": "completed"})
        self.marker.chmod(0o644)
        with self.assertRaises(NUDGE.ConfigError):
            NUDGE.load_marker(self.marker)

    def test_environment_allowlist_drops_credentials(self):
        sanitized = NUDGE.sanitized_environment(self.environment)
        self.assertEqual(sanitized["HOME"], str(self.root))
        self.assertNotIn("MERISTEM_TOKEN", sanitized)
        self.assertNotIn("OPENAI_API_KEY", sanitized)
        self.assertNotIn("TEST_SENTINEL_SECRET", sanitized)

    def test_constructor_signal_window_closes_spawned_process_group(self):
        process = mock.Mock()
        process.pid = 424242
        process.stdin = mock.Mock()
        process.stdout = mock.Mock()
        process.poll.return_value = None
        selector = mock.Mock()
        selector.register.side_effect = NUDGE.TerminationRequested(143)

        with mock.patch.object(NUDGE.subprocess, "Popen", return_value=process), \
             mock.patch.object(NUDGE.selectors, "DefaultSelector", return_value=selector), \
             mock.patch.object(NUDGE.os, "killpg") as killpg:
            with self.assertRaises(NUDGE.TerminationRequested):
                NUDGE.AppServerClient(
                    [sys.executable], self.root, environment=self.environment
                )

        process.stdin.close.assert_called_once_with()
        killpg.assert_called_once_with(process.pid, NUDGE.signal.SIGTERM)
        process.wait.assert_called_once_with(timeout=3)
        selector.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
