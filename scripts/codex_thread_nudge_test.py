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

scenario, record_path, reconcile_client_id, request_method = sys.argv[1:5]
methods = []
responses = []
prompts = []
turn_start_count = 0
pending_admission = None

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
        json.dump({
            "methods": methods,
            "responses": responses,
            "prompts": prompts,
            "sensitive_env": sensitive,
            "turn_start_count": turn_start_count,
        }, handle)

def send_admission_response(admission):
    if admission["method"] == "turn/steer":
        result = {"turnId": admission["turn_id"]}
    else:
        result = {"turn": {"id": admission["turn_id"]}}
    send({"id": admission["request_id"], "result": result})

def send_completion(admission):
    send({
        "method": "turn/completed",
        "params": {
            "threadId": admission["thread_id"],
            "turn": {
                "id": admission["turn_id"],
                "status": "completed",
                "items": [],
            },
        },
    })

for raw in sys.stdin:
    message = json.loads(raw)
    method = message.get("method")
    if method:
        methods.append(method)
    elif "id" in message:
        responses.append(message)
        if pending_admission is not None:
            if scenario == "server_request_before":
                send_admission_response(pending_admission)
            if scenario == "server_request_eof":
                record()
                break
            if scenario == "trusted_mcp_approval":
                # Persist the accepted response before completion lets the
                # client close the fake app-server process.
                record()
            send_completion(pending_admission)
            if scenario != "trusted_mcp_approval":
                record()
            pending_admission = None
        else:
            record()
        continue
    if method == "initialize":
        send({"id": message["id"], "result": {"secret": "RAW-SECRET-SENTINEL"}})
    elif method == "initialized":
        continue
    elif method == "thread/resume":
        thread_id = message["params"]["threadId"]
        if scenario == "resume_rejected":
            record()
            send({
                "id": message["id"],
                "error": {"code": -32000, "message": "RAW-SECRET-SENTINEL"},
            })
            continue
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
        turn_start_count += 1
        for item in message.get("params", {}).get("input", []):
            if isinstance(item, dict) and item.get("type") == "text":
                prompts.append(item.get("text"))
        thread_id = message["params"]["threadId"]
        turn_id = "active-turn" if method == "turn/steer" else "started-turn"
        if scenario in (
            "server_request_before",
            "server_request_after",
            "server_request_eof",
            "trusted_mcp_approval",
        ):
            pending_admission = {
                "method": method,
                "request_id": message["id"],
                "thread_id": thread_id,
                "turn_id": turn_id,
            }
            if scenario != "server_request_before":
                send_admission_response(pending_admission)
            # Deliberately collide ids in the two JSON-RPC directions and put
            # raw-looking data in params. The client must neither confuse the
            # response nor reflect the params into output/diagnostics.
            request_params = {"secret": "RAW-SERVER-REQUEST-SECRET-SENTINEL"}
            if scenario == "trusted_mcp_approval":
                request_params = {
                    "serverName": "meristem_listener",
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "mode": "form",
                    "message": "Allow the meristem_listener MCP server to run tool \"work_items.held_assignments\"?",
                    "requestedSchema": {"type": "object", "properties": {}},
                }
            send({
                "id": message["id"],
                "method": request_method,
                "params": request_params,
            })
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
        # Persist the metadata before sending the response/completion. The
        # client may close the app-server immediately after consuming an
        # already-buffered completion; recording afterward would make this
        # fixture itself race the close path.
        record()
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
    else:
        record()
'''


EXPECTED_SERVER_REQUEST_RESULTS = {
    "item/commandExecution/requestApproval": {
        "result": {"decision": "decline"}
    },
    "item/fileChange/requestApproval": {"result": {"decision": "decline"}},
    "item/tool/requestUserInput": {
        "error": {
            "code": -32601,
            "message": "server request unsupported by unattended client",
        }
    },
    "mcpServer/elicitation/request": {
        "result": {"action": "decline", "content": None}
    },
    "item/permissions/requestApproval": {
        "result": {"permissions": {}, "scope": "turn"}
    },
    "item/tool/call": {"result": {"contentItems": [], "success": False}},
    "account/chatgptAuthTokens/refresh": {
        "error": {
            "code": -32601,
            "message": "server request unsupported by unattended client",
        }
    },
    "attestation/generate": {
        "error": {
            "code": -32601,
            "message": "server request unsupported by unattended client",
        }
    },
    "currentTime/read": {
        "error": {
            "code": -32601,
            "message": "server request unsupported by unattended client",
        }
    },
    "applyPatchApproval": {"result": {"decision": "denied"}},
    "execCommandApproval": {"result": {"decision": "denied"}},
}


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
            "CODEX_THREAD_ID": self.thread_id,
            "CODEX_MERISTEM_TOKEN_FILE": "/scoped/listener/token",
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

    def command(
        self,
        scenario,
        reconcile_client_id="none",
        request_method="item/commandExecution/requestApproval",
    ):
        return [
            sys.executable,
            str(self.fake),
            scenario,
            str(self.record),
            reconcile_client_id,
            request_method,
        ]

    def marker_value(self):
        return json.loads(self.marker.read_text(encoding="utf-8"))

    def record_value(self):
        for _ in range(50):
            try:
                return json.loads(self.record.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.01)
        return json.loads(self.record.read_text(encoding="utf-8"))

    def activation_args(self, mode="dispatch"):
        return types.SimpleNamespace(
            activation_id="019fc9ec-2d6b-7861-af0e-c1a8b540d5b7",
            assignment_event_id="019fc9ec-2d6b-7861-af0e-c1a8b540d5b8",
            mode=mode,
            codex_bin=sys.executable,
            thread_id=self.thread_id,
            repo_root=str(self.root),
            request_timeout=2.0,
            completion_timeout=2.0,
            approved_mcp_server_name=None,
            approved_mcp_tool=[],
        )

    def test_activation_dispatch_is_metadata_only_and_journal_free(self):
        args = self.activation_args()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args, self.command("idle"), environment=self.environment
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        receipts = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(
            [receipt["outcome"] for receipt in receipts],
            ["accepted", "completed"],
        )
        self.assertFalse(self.marker.exists())
        record = self.record_value()
        self.assertEqual(record["methods"].count("turn/start"), 1)
        self.assertNotIn("turn/steer", record["methods"])
        self.assertEqual(record["sensitive_env"], [])
        self.assertEqual(len(record["prompts"]), 1)
        prompt = record["prompts"][0]
        self.assertIn(args.activation_id, prompt)
        self.assertIn(args.assignment_event_id, prompt)
        self.assertNotIn(self.batch.read_text(encoding="utf-8"), prompt)
        self.assertNotIn("event_count", prompt)

    def test_activation_reconcile_never_resubmits(self):
        args = self.activation_args("reconcile")
        client_id = NUDGE.activation_client_message_id(args.activation_id)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args,
                self.command("reconcile", client_id),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(
            [json.loads(line)["outcome"] for line in stdout.getvalue().splitlines()],
            ["completed"],
        )
        self.assertFalse(self.marker.exists())
        self.assertFalse(self.record.exists())

    def test_activation_reconcile_absence_stays_ambiguous(self):
        args = self.activation_args("reconcile")
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.activate(args, self.command("idle"), environment=self.environment)
        self.assertFalse(self.marker.exists())

    def test_activation_active_task_is_not_steered(self):
        args = self.activation_args()
        with self.assertRaises(NUDGE.TargetBusy):
            NUDGE.activate(args, self.command("active"), environment=self.environment)
        self.assertFalse(self.marker.exists())

    def test_activation_resume_rejection_before_admission_is_busy(self):
        args = self.activation_args()
        with self.assertRaises(NUDGE.TargetBusy):
            NUDGE.activate(
                args, self.command("resume_rejected"), environment=self.environment
            )
        record = self.record_value()
        self.assertNotIn("turn/start", record["methods"])
        self.assertFalse(self.marker.exists())

    def test_activation_approves_one_exact_bound_mcp_tool(self):
        args = self.activation_args()
        args.approved_mcp_server_name = "meristem_listener"
        args.approved_mcp_tool = ["work_items.held_assignments"]
        result = NUDGE.activate(
            args,
            self.command(
                "trusted_mcp_approval",
                request_method="mcpServer/elicitation/request",
            ),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        record = self.record_value()
        self.assertEqual(
            record["responses"],
            [{"id": 3, "result": {"action": "accept", "content": {}}}],
        )

    def test_mcp_approval_wrong_tool_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
        client.approved_mcp_server_name = "meristem_listener"
        client.approved_mcp_tools = frozenset({"work_items.get"})
        client.approved_thread_id = self.thread_id
        client.last_server_request_method = None
        client._send = mock.Mock()
        client._respond_to_server_request({
            "id": 7,
            "method": "mcpServer/elicitation/request",
            "params": {
                "serverName": "meristem_listener",
                "threadId": self.thread_id,
                "turnId": "turn-1",
                "mode": "form",
                "message": "Allow the meristem_listener MCP server to run tool \"work_items.transition\"?",
                "requestedSchema": {"type": "object", "properties": {}},
            },
        })
        client._send.assert_called_once_with({
            "id": 7,
            "result": {"action": "decline", "content": None},
        })

    def test_mcp_approval_shape_drift_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
        client.approved_mcp_server_name = "meristem_listener"
        client.approved_mcp_tools = frozenset({"work_items.get"})
        client.approved_thread_id = self.thread_id
        client.last_server_request_method = None
        client._send = mock.Mock()
        client._respond_to_server_request({
            "id": 8,
            "method": "mcpServer/elicitation/request",
            "params": {
                "serverName": "meristem_listener",
                "threadId": self.thread_id,
                "turnId": "turn-1",
                "mode": "form",
                "message": "Please allow work_items.get",
                "requestedSchema": {"type": "object", "properties": {}},
            },
        })
        client._send.assert_called_once_with({
            "id": 8,
            "result": {"action": "decline", "content": None},
        })

    def test_activation_busy_has_distinct_structural_exit(self):
        args = self.activation_args()
        argv = [
            "activate",
            "--codex-bin",
            sys.executable,
            "--thread-id",
            self.thread_id,
            "--repo-root",
            str(self.root),
            "--activation-id",
            args.activation_id,
            "--assignment-event-id",
            args.assignment_event_id,
            "--mode",
            "dispatch",
            "--diagnostic",
        ]
        stdout = io.StringIO()
        with mock.patch.object(NUDGE, "activate", side_effect=NUDGE.TargetBusy()), \
             mock.patch.object(NUDGE.signal, "signal"), \
             contextlib.redirect_stdout(stdout):
            result = NUDGE.main(argv)
        self.assertEqual(result, NUDGE.EXIT_BUSY)
        self.assertEqual(json.loads(stdout.getvalue())["failure_class"], "TargetBusy")

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

    def test_server_request_before_admission_is_denied_without_duplicate(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        method = "item/permissions/requestApproval"
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = NUDGE.deliver(
                self.args,
                self.command("server_request_before", request_method=method),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(self.marker_value()["state"], "completed")
        record = json.loads(self.record.read_text(encoding="utf-8"))
        self.assertEqual(record["turn_start_count"], 1)
        self.assertEqual(record["methods"].count("turn/start"), 1)
        self.assertEqual(
            record["responses"],
            [{"id": 3, **EXPECTED_SERVER_REQUEST_RESULTS[method]}],
        )
        emitted = stdout.getvalue() + stderr.getvalue()
        self.assertNotIn("SECRET-SENTINEL", emitted)

    def test_all_schema_server_requests_are_safely_denied_after_admission(self):
        self.assertEqual(
            set(EXPECTED_SERVER_REQUEST_RESULTS), set(NUDGE.SERVER_REQUEST_METHODS)
        )
        for method, expected in EXPECTED_SERVER_REQUEST_RESULTS.items():
            with self.subTest(method=method):
                if self.marker.exists():
                    self.marker.unlink()
                if self.record.exists():
                    self.record.unlink()
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
                    stderr
                ):
                    result = NUDGE.deliver(
                        self.args,
                        self.command("server_request_after", request_method=method),
                        environment=self.environment,
                    )
                self.assertEqual(result, NUDGE.EXIT_OK)
                self.assertEqual(self.marker_value()["state"], "completed")
                record = json.loads(self.record.read_text(encoding="utf-8"))
                self.assertEqual(record["turn_start_count"], 1)
                self.assertEqual(record["methods"].count("turn/start"), 1)
                self.assertEqual(record["responses"], [{"id": 3, **expected}])
                serialized = json.dumps(record["responses"], sort_keys=True)
                self.assertNotIn("accessToken", serialized)
                self.assertNotIn("currentTimeAt", serialized)
                self.assertNotIn("approved", serialized)
                emitted = stdout.getvalue() + stderr.getvalue()
                self.assertNotIn("SECRET-SENTINEL", emitted)

    def test_unknown_server_request_gets_generic_error_and_no_second_turn(self):
        result = NUDGE.deliver(
            self.args,
            self.command(
                "server_request_after", request_method="future/raw-secret-method"
            ),
            environment=self.environment,
        )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(self.marker_value()["state"], "completed")
        record = json.loads(self.record.read_text(encoding="utf-8"))
        self.assertEqual(record["turn_start_count"], 1)
        self.assertEqual(record["methods"].count("turn/start"), 1)
        self.assertEqual(
            record["responses"],
            [
                {
                    "id": 3,
                    "error": {
                        "code": -32601,
                        "message": "server request unsupported by unattended client",
                    },
                }
            ],
        )
        self.assertNotIn("future/raw-secret-method", json.dumps(record["responses"]))

    def test_eof_diagnostic_is_allowlisted_and_secret_silent(self):
        method = "account/chatgptAuthTokens/refresh"
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaises(NUDGE.TransportError) as caught:
                NUDGE.deliver(
                    self.args,
                    self.command("server_request_eof", request_method=method),
                    environment=self.environment,
                )
        error = caught.exception
        self.assertEqual(error.nudge_stage, "completion")
        self.assertEqual(error.nudge_server_request_method, method)
        self.assertEqual(
            NUDGE._diagnostic_payload(error, "unknown"),
            {
                "failure_class": "TransportError",
                "server_request_method": method,
                "stage": "completion",
            },
        )
        self.assertEqual(self.marker_value()["state"], "accepted")
        record = json.loads(self.record.read_text(encoding="utf-8"))
        self.assertEqual(
            record["responses"],
            [{"id": 3, **EXPECTED_SERVER_REQUEST_RESULTS[method]}],
        )
        self.assertNotIn("SECRET-SENTINEL", stdout.getvalue() + stderr.getvalue())

    def test_main_diagnostic_prints_only_allowlisted_metadata(self):
        NUDGE.atomic_write_json(self.marker, {"state": "accepted"})
        error = NUDGE.TransportError("RAW-SECRET-SENTINEL", 99)
        error.nudge_stage = "completion"
        error.nudge_server_request_method = "unknown"
        argv = [
            "deliver",
            "--codex-bin",
            sys.executable,
            "--thread-id",
            self.thread_id,
            "--repo-root",
            str(self.root),
            "--batch-file",
            str(self.batch),
            "--marker-file",
            str(self.marker),
            "--diagnostic",
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(NUDGE, "deliver", side_effect=error), mock.patch.object(
            NUDGE.signal, "signal"
        ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = NUDGE.main(argv)
        self.assertEqual(result, NUDGE.EXIT_AMBIGUOUS)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "failure_class": "TransportError",
                "server_request_method": "unknown",
                "stage": "completion",
            },
        )
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn("SECRET-SENTINEL", stdout.getvalue())

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
        self.assertEqual(
            sanitized["CODEX_MERISTEM_TOKEN_FILE"], "/scoped/listener/token"
        )
        self.assertEqual(sanitized["CODEX_THREAD_ID"], self.thread_id)
        self.assertNotIn("MERISTEM_TOKEN", sanitized)
        self.assertNotIn("MERISTEM_TOKEN_FILE", sanitized)
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
