#!/usr/bin/env python3
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import tempfile
import textwrap
import types
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("codex-thread-nudge.py")
WRAPPER_PATH = Path(__file__).with_name("codex-listener-app-server.sh")
MCP_COMMAND_TEST_PATH = Path(__file__).with_name(
    "codex_listener_mcp_command_test.sh"
)
SPEC = importlib.util.spec_from_file_location("codex_thread_nudge", MODULE_PATH)
NUDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NUDGE)


FAKE_APP_SERVER = r'''#!/usr/bin/env python3
import json
import os
import sys
import time

scenario, record_path, reconcile_client_id, request_method, expected_mcp_command = sys.argv[1:6]
methods = []
responses = []
prompts = []
turn_start_count = 0
pending_admission = None
mcp_status_calls = 0

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
    temporary_path = record_path + ".tmp"
    with open(temporary_path, "w", encoding="utf-8") as handle:
        json.dump({
            "methods": methods,
            "responses": responses,
            "prompts": prompts,
            "sensitive_env": sensitive,
            "listener_binding_env": {
                key: os.environ.get(key) for key in (
                    "CODEX_THREAD_ID",
                    "MERISTEM_LISTENER_CODEX_HOME",
                    "MERISTEM_LISTENER_CODEX_SQLITE_HOME",
                    "MERISTEM_MCP_EXPECT_ACTOR_ID",
                    "MERISTEM_MCP_LISTENER_ACTIVATION_ID",
                    "MERISTEM_MCP_LISTENER_WORK_ITEM_ID",
                    "MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID",
                )
            },
            "ambient_codex_env": [
                key for key in (
                    "CODEX_HOME", "CODEX_CI",
                    "CODEX_INTERNAL_ORIGINATOR_OVERRIDE", "CODEX_SANDBOX",
                    "CODEX_SANDBOX_NETWORK_DISABLED", "CODEX_SHELL",
                ) if key in os.environ
            ],
            "turn_start_count": turn_start_count,
        }, handle)
    os.replace(temporary_path, record_path)

def send_admission_response(admission):
    if admission["method"] == "turn/steer":
        result = {"turnId": admission["turn_id"]}
    else:
        result = {"turn": {"id": admission["turn_id"]}}
    send({"id": admission["request_id"], "result": result})

def send_completion(admission):
    status = "completed"
    if scenario == "trusted_mcp_approval_failed":
        status = "failed"
    elif scenario == "trusted_mcp_approval_interrupted":
        status = "interrupted"
    send({
        "method": "turn/completed",
        "params": {
            "threadId": admission["thread_id"],
            "turn": {
                "id": admission["turn_id"],
                "status": status,
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
            if scenario in (
                "server_request_before",
                "trusted_mcp_approval_before_admission",
            ):
                send_admission_response(pending_admission)
            if scenario == "server_request_eof":
                record()
                break
            # Persist the response before completion: the real client may
            # close the fake server immediately after the terminal notice.
            record()
            send_completion(pending_admission)
            pending_admission = None
        else:
            record()
        continue
    if method == "initialize":
        user_agent = "Codex Desktop/0.147.0-alpha.6.5 (fixture)"
        if scenario == "invalid_initialize_identity":
            user_agent = "Codex Desktop/0.147.0-alpha.6.5\nraw"
        send({
            "id": message["id"],
            "result": {
                "codexHome": "/secret/codex-home",
                "platformFamily": "unix",
                "platformOs": "test",
                "userAgent": user_agent,
                "secret": "RAW-SECRET-SENTINEL",
            },
        })
    elif method == "initialized":
        continue
    elif method == "config/read":
        servers = {
            "meristem_listener": {
                "args": [],
                "command": expected_mcp_command,
                "enabled": True,
                "enabled_tools": [
                    "work_items.append_event",
                    "work_items.get",
                    "work_items.get_assignment",
                ],
            },
        }
        if os.environ.get("MERISTEM_LISTENER_PROBE") == "1":
            servers["meristem_listener"] = {
                "args": [],
                "command": "/usr/bin/false",
                "enabled": False,
            }
        if scenario == "ambient_mcp_config":
            servers["ambient"] = {
                "args": [],
                "command": "/unsafe/ambient",
                "enabled": True,
            }
        record()
        send({
            "id": message["id"],
            "result": {
                "config": {
                    "features": {
                        "apps": scenario == "apps_enabled_config",
                    },
                    "mcp_servers": servers,
                },
                "origins": {
                    "features.apps": {
                        "name": {"type": "sessionFlags"},
                        "version": "sha256:fixture",
                    },
                    "mcp_servers.meristem_listener.command": {
                        "name": {"type": "sessionFlags"},
                        "version": "sha256:fixture",
                    }
                },
            },
        })
    elif method == "mcpServerStatus/list":
        mcp_status_calls += 1
        tool_names = [
            "work_items.append_event",
            "work_items.get",
            "work_items.get_assignment",
        ]
        if scenario == "wrong_listener_tools":
            tool_names.append("work_items.transition")
        server_info = {
            "name": "meristem",
            "version": "fixture",
            "description": "meristem-actor-id-v1:" + os.environ.get("MERISTEM_MCP_EXPECT_ACTOR_ID", "missing"),
        }
        if scenario == "missing_task_attestation":
            server_info.pop("description")
        elif scenario == "wrong_task_attestation" or (
            scenario == "thread_wrong_task_attestation"
            and message.get("params", {}).get("threadId") is not None
        ):
            server_info["description"] = "meristem-actor-id-v1:019fc9ec-2d6b-7861-af0e-c1a8b540d5ff"
        if scenario == "mcp_status_stuck_starting" or (
            scenario == "delayed_mcp_status" and mcp_status_calls <= 2
        ):
            server_info = None
            tool_names = []
        data = [{
            "authStatus": "unsupported",
            "name": "meristem_listener",
            "resourceTemplates": [],
            "resources": [],
            "serverInfo": server_info,
            "tools": {name: {"name": name} for name in tool_names},
        }]
        if scenario == "ambient_mcp_status" or (
            scenario == "thread_ambient_mcp_status"
            and message.get("params", {}).get("threadId") is not None
        ):
            data.append({
                "authStatus": "unsupported",
                "name": "ambient",
                "resourceTemplates": [],
                "resources": [],
                "serverInfo": {"name": "ambient", "version": "fixture"},
                "tools": {},
            })
        record()
        send({"id": message["id"], "result": {"data": data, "nextCursor": None}})
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
        elif scenario in ("reconcile", "reconcile_server_request"):
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
        if scenario == "reconcile_server_request":
            send({
                "id": message["id"],
                "method": request_method,
                "params": {"secret": "RAW-SERVER-REQUEST-SECRET-SENTINEL"},
            })
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
            "trusted_mcp_approval_failed",
            "trusted_mcp_approval_interrupted",
            "trusted_mcp_approval_before_admission",
        ):
            pending_admission = {
                "method": method,
                "request_id": message["id"],
                "thread_id": thread_id,
                "turn_id": turn_id,
            }
            if scenario not in (
                "server_request_before",
                "trusted_mcp_approval_before_admission",
            ):
                send_admission_response(pending_admission)
            # Deliberately collide ids in the two JSON-RPC directions and put
            # raw-looking data in params. The client must neither confuse the
            # response nor reflect the params into output/diagnostics.
            request_params = {"secret": "RAW-SERVER-REQUEST-SECRET-SENTINEL"}
            if scenario in (
                "trusted_mcp_approval",
                "trusted_mcp_approval_before_admission",
            ):
                request_params = {
                    "serverName": "meristem_listener",
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "mode": "form",
                    "message": "Allow the meristem_listener MCP server to run tool \"work_items.append_event\"?",
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
    "applyPatchApproval": {"result": {"decision": "abort"}},
    "execCommandApproval": {"result": {"decision": "abort"}},
}


class NudgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.batch = self.root / "delivery.tsv"
        self.batch.write_text("event\tactor\tkind\tsubject\n", encoding="utf-8")
        self.batch.chmod(0o600)
        self.marker = self.root / "delivery.json"
        self.fake = self.root / "fake-app-server.py"
        self.fake.write_text(textwrap.dedent(FAKE_APP_SERVER), encoding="utf-8")
        self.fake.chmod(0o700)
        self.record = self.root / "record.json"
        self.thread_id = "019f6309-db25-75c2-b87d-41d3050581db"
        self.work_item_id = "019fc9ec-2d6b-7861-af0e-c1a8b540d5b9"
        self.task_principal_token_id = "019fc9ec-2d6b-7861-af0e-c1a8b540d5ba"
        self.listener_codex_home = self.root / "listener-runtime-home"
        self.listener_codex_home.mkdir(mode=0o700)
        self.listener_codex_sqlite_home = self.root / "primary-codex-state"
        self.listener_codex_sqlite_home.mkdir(mode=0o700)
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
            "MERISTEM_MCP_EXPECT_ACTOR_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5ff",
            "MERISTEM_MCP_LISTENER_ACTIVATION_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5fe",
            "MERISTEM_MCP_LISTENER_WORK_ITEM_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5fd",
            "MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5fc",
            "MERISTEM_TOKEN": "secret",
            "MERISTEM_TOKEN_FILE": "/secret/token",
            "MERISTEM_DATABASE_URL": "postgres://secret",
            "ROOT_TOKEN_FILE": "/secret/root",
            "PGPASSWORD": "secret",
            "OPENAI_API_KEY": "secret",
            "ANTHROPIC_API_KEY": "secret",
            "TEST_SENTINEL_SECRET": "secret",
            "CODEX_HOME": "/ambient/codex-home",
            "CODEX_CI": "ambient",
            "CODEX_INTERNAL_ORIGINATOR_OVERRIDE": "ambient",
            "CODEX_SANDBOX": "ambient",
            "CODEX_SANDBOX_NETWORK_DISABLED": "ambient",
            "CODEX_SHELL": "/ambient/shell",
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
            str(MCP_COMMAND_TEST_PATH.with_name("codex-listener-mcp-command.sh")),
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
            command="activate",
            activation_id="019fc9ec-2d6b-7861-af0e-c1a8b540d5b7",
            work_item_id=self.work_item_id,
            assignment_event_id="019fc9ec-2d6b-7861-af0e-c1a8b540d5b8",
            task_principal_token_id=self.task_principal_token_id,
            mode=mode,
            codex_bin=sys.executable,
            thread_id=self.thread_id,
            listener_codex_home=str(self.listener_codex_home),
            listener_codex_sqlite_home=str(self.listener_codex_sqlite_home),
            repo_root=str(self.root),
            request_timeout=2.0,
            completion_timeout=2.0,
        )

    def listener_wrapper_fixture(self, token_mode=0o600):
        mcp_command = WRAPPER_PATH.with_name("codex-listener-mcp-command.sh")
        stale_override = self.root / "stale-meristem-listener-mcp"
        stale_override.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        stale_override.chmod(0o700)
        codex_bin = self.root / "codex-fixture"
        codex_bin.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$@\" > \"$CODEX_WRAPPER_RECORD\"\n"
            "printf '%s\\n' \"$CODEX_HOME\" \"$CODEX_SQLITE_HOME\" > \"$CODEX_WRAPPER_ENV_RECORD\"\n",
            encoding="utf-8",
        )
        codex_bin.chmod(0o700)
        wrapper_record = self.root / "wrapper-argv.txt"
        primary_codex_home = self.root / ".codex"
        primary_codex_home.mkdir(exist_ok=True)
        (primary_codex_home / "auth.json").write_text("{}\n", encoding="utf-8")
        (primary_codex_home / "thread-writer-locks").mkdir(exist_ok=True)
        listener_codex_home = self.root / "listener-codex-home"
        listener_codex_home.mkdir(exist_ok=True)
        listener_codex_home.chmod(0o700)
        token = listener_codex_home / "meristem-task.token"
        token.write_text("TOKEN-CONTENT-MUST-STAY-LOCAL\n", encoding="utf-8")
        token.chmod(token_mode)
        (listener_codex_home / "auth.json").symlink_to(
            primary_codex_home / "auth.json"
        )
        (listener_codex_home / "thread-writer-locks").symlink_to(
            primary_codex_home / "thread-writer-locks",
            target_is_directory=True,
        )
        environment = {
            "HOME": str(self.root),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CODEX_BIN": str(codex_bin),
            # Historical overrides must not replace the reviewed sibling.
            "MERISTEM_LISTENER_MCP_COMMAND": str(stale_override),
            "MERISTEM_MCP_EXPECT_ACTOR_ID": self.task_principal_token_id,
            "MERISTEM_MCP_LISTENER_ACTIVATION_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5b7",
            "MERISTEM_MCP_LISTENER_WORK_ITEM_ID": self.work_item_id,
            "MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID": "019fc9ec-2d6b-7861-af0e-c1a8b540d5b8",
            "CODEX_MERISTEM_TOKEN_FILE": "/ambient/must-not-pass.token",
            "CODEX_WRAPPER_RECORD": str(wrapper_record),
            "CODEX_WRAPPER_ENV_RECORD": str(self.root / "wrapper-env.txt"),
            "MERISTEM_LISTENER_CODEX_HOME": str(listener_codex_home),
            "MERISTEM_LISTENER_CODEX_SQLITE_HOME": str(primary_codex_home),
        }
        return token, mcp_command, wrapper_record, environment

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
        self.assertIn(args.work_item_id, prompt)
        self.assertIn(args.assignment_event_id, prompt)
        self.assertIn(args.task_principal_token_id, prompt)
        self.assertNotIn(self.batch.read_text(encoding="utf-8"), prompt)
        self.assertNotIn("event_count", prompt)

    def test_activation_reconcile_never_resubmits(self):
        args = self.activation_args("reconcile")
        client_id = NUDGE.activation_client_message_id(args.activation_id)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), self.assertRaises(
            NUDGE.CompletionTimeout
        ):
            NUDGE.activate(
                args,
                self.command("reconcile", client_id),
                environment=self.environment,
            )
        self.assertEqual(stdout.getvalue(), "")
        self.assertFalse(self.marker.exists())
        self.assertNotIn("turn/start", self.record_value()["methods"])

    def test_activation_reconcile_absence_stays_ambiguous(self):
        args = self.activation_args("reconcile")
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.activate(args, self.command("idle"), environment=self.environment)
        self.assertFalse(self.marker.exists())

    def test_activation_rejects_ambient_mcp_config_before_resume(self):
        for scenario in ("ambient_mcp_config", "apps_enabled_config"):
            with self.subTest(scenario=scenario):
                self.record.unlink(missing_ok=True)
                with self.assertRaises(NUDGE.ProtocolError):
                    NUDGE.activate(
                        self.activation_args(),
                        self.command(scenario),
                        environment=self.environment,
                    )
                self.assertNotIn("thread/resume", self.record_value()["methods"])

    def test_activation_rejects_extra_server_or_tool_before_resume(self):
        for scenario in (
            "ambient_mcp_status",
            "wrong_listener_tools",
            "missing_task_attestation",
            "wrong_task_attestation",
        ):
            with self.subTest(scenario=scenario):
                self.record.unlink(missing_ok=True)
                with self.assertRaises(NUDGE.ProtocolError):
                    NUDGE.activate(
                        self.activation_args(),
                        self.command(scenario),
                        environment=self.environment,
                    )
                self.assertNotIn("thread/resume", self.record_value()["methods"])

    def test_activation_waits_for_exact_listener_mcp_starting_shape(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                self.activation_args(),
                self.command("delayed_mcp_status"),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertIn("thread/resume", self.record_value()["methods"])
        self.assertIn("turn/start", self.record_value()["methods"])

    def test_activation_times_out_while_listener_mcp_stays_starting(self):
        args = self.activation_args()
        args.request_timeout = 0.2
        with self.assertRaises(NUDGE.CompletionTimeout):
            NUDGE.activate(
                args,
                self.command("mcp_status_stuck_starting"),
                environment=self.environment,
            )
        methods = self.record_value()["methods"]
        self.assertIn("thread/resume", methods)
        self.assertNotIn("turn/start", methods)

    def test_activation_rejects_thread_effective_mcp_drift_before_start(self):
        for scenario in (
            "thread_ambient_mcp_status",
            "thread_wrong_task_attestation",
        ):
            with self.subTest(scenario=scenario):
                self.record.unlink(missing_ok=True)
                with self.assertRaises(NUDGE.ProtocolError):
                    NUDGE.activate(
                        self.activation_args(),
                        self.command(scenario),
                        environment=self.environment,
                    )
                methods = self.record_value()["methods"]
                self.assertIn("thread/resume", methods)
                self.assertNotIn("turn/start", methods)

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

    def test_activation_overwrites_ambient_codex_binding_from_reviewed_args(self):
        args = self.activation_args()
        environment = dict(self.environment)
        environment["CODEX_THREAD_ID"] = "019f6309-db25-75c2-b87d-41d3050581ff"
        environment["MERISTEM_LISTENER_CODEX_HOME"] = "/ambient/wrong-home"
        environment["MERISTEM_LISTENER_CODEX_SQLITE_HOME"] = "/ambient/wrong-state"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args, self.command("idle"), environment=environment
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        record = self.record_value()
        self.assertEqual(
            record["listener_binding_env"],
            {
                    "CODEX_THREAD_ID": self.thread_id,
                    "MERISTEM_LISTENER_CODEX_HOME": str(self.listener_codex_home),
                    "MERISTEM_LISTENER_CODEX_SQLITE_HOME": str(
                        self.listener_codex_sqlite_home
                    ),
                    "MERISTEM_MCP_EXPECT_ACTOR_ID": args.task_principal_token_id,
                    "MERISTEM_MCP_LISTENER_ACTIVATION_ID": args.activation_id,
                    "MERISTEM_MCP_LISTENER_WORK_ITEM_ID": args.work_item_id,
                    "MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID": args.assignment_event_id,
            },
        )
        self.assertEqual(record["ambient_codex_env"], [])

    def test_probe_reports_validated_app_server_identity(self):
        args = self.activation_args()
        args.command = "probe"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.probe(
                args, self.command("idle"), environment=self.environment
            )
        self.assertEqual(result, NUDGE.EXIT_OK)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "active_turn_count": 0,
                "app_server_user_agent": "Codex Desktop/0.147.0-alpha.6.5 (fixture)",
                "thread_status": "idle",
                "waiting": False,
            },
        )

    def test_probe_rejects_unprintable_app_server_identity(self):
        args = self.activation_args()
        args.command = "probe"
        with self.assertRaises(NUDGE.ProtocolError):
            NUDGE.probe(
                args,
                self.command("invalid_initialize_identity"),
                environment=self.environment,
            )

    def test_listener_wrapper_accepts_exact_mode_0600_token(self):
        token, mcp_command, wrapper_record, environment = (
            self.listener_wrapper_fixture()
        )
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            wrapper_record.read_text(encoding="utf-8").splitlines(),
            [
                "--config",
                "features.apps=false",
                "--config",
                (
                    "mcp_servers.meristem_listener={"
                    f'command="{mcp_command}",enabled_tools=['
                    '"work_items.append_event","work_items.get",'
                    '"work_items.get_assignment"],env={'
                    f'CODEX_HOME="{environment["MERISTEM_LISTENER_CODEX_HOME"]}",'
                    f'CODEX_MERISTEM_TOKEN_FILE="{token}",'
                    f'MERISTEM_MCP_EXPECT_ACTOR_ID="{environment["MERISTEM_MCP_EXPECT_ACTOR_ID"]}",'
                    f'MERISTEM_MCP_LISTENER_ACTIVATION_ID="{environment["MERISTEM_MCP_LISTENER_ACTIVATION_ID"]}",'
                    f'MERISTEM_MCP_LISTENER_WORK_ITEM_ID="{environment["MERISTEM_MCP_LISTENER_WORK_ITEM_ID"]}",'
                    f'MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID="{environment["MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID"]}"}}}}'
                ),
                "app-server",
                "--stdio",
            ],
        )
        self.assertEqual(
            (self.root / "wrapper-env.txt").read_text(encoding="utf-8").splitlines(),
            [
                environment["MERISTEM_LISTENER_CODEX_HOME"],
                environment["MERISTEM_LISTENER_CODEX_SQLITE_HOME"],
            ],
        )
        self.assertNotIn("TOKEN-CONTENT-MUST-STAY-LOCAL", completed.stderr)

    def test_listener_wrapper_rejects_nonisolated_codex_home(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        listener_home = Path(environment["MERISTEM_LISTENER_CODEX_HOME"])
        (listener_home / "config.toml").write_text(
            "[mcp_servers.ambient]\ncommand='unsafe'\n", encoding="utf-8"
        )
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("contain no config.toml", completed.stderr)
        self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_rejects_nonprivate_codex_home(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        listener_home = Path(environment["MERISTEM_LISTENER_CODEX_HOME"])
        listener_home.chmod(0o750)
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("must have mode 0700", completed.stderr)
        self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_rejects_symlinked_codex_home(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        listener_home = Path(environment["MERISTEM_LISTENER_CODEX_HOME"])
        linked_home = self.root / "listener-codex-home-link"
        linked_home.symlink_to(listener_home, target_is_directory=True)
        environment["MERISTEM_LISTENER_CODEX_HOME"] = str(linked_home)
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("missing absolute dedicated listener CODEX_HOME", completed.stderr)
        self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_rejects_symlinked_parent_of_codex_home(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        listener_home = Path(environment["MERISTEM_LISTENER_CODEX_HOME"])
        real_parent = self.root / "real-listener-parent"
        real_parent.mkdir()
        moved_home = real_parent / "listener-codex-home"
        listener_home.rename(moved_home)
        linked_parent = self.root / "linked-listener-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        environment["MERISTEM_LISTENER_CODEX_HOME"] = str(
            linked_parent / "listener-codex-home"
        )
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("exact symlink-free path", completed.stderr)
        self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_rejects_every_other_codex_argv(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        for argv in (
            ["app-server"],
            ["exec"],
            ["app-server", "--stdio", "extra"],
        ):
            with self.subTest(argv=argv):
                wrapper_record.unlink(missing_ok=True)
                completed = subprocess.run(
                    [str(WRAPPER_PATH), *argv],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
                self.assertIn("only permits: app-server --stdio", completed.stderr)
                self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_rejects_group_readable_token(self):
        token, _, wrapper_record, environment = self.listener_wrapper_fixture(0o640)
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("must have mode 0600", completed.stderr)
        self.assertNotIn("TOKEN-CONTENT-MUST-STAY-LOCAL", completed.stderr)
        self.assertFalse(wrapper_record.exists())
        self.assertEqual(stat.S_IMODE(token.stat().st_mode), 0o640)

    def test_listener_wrapper_rejects_symlinked_token(self):
        token, _, wrapper_record, environment = self.listener_wrapper_fixture()
        real_token = self.root / "listener-token-real"
        token.rename(real_token)
        token.symlink_to(real_token)
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, NUDGE.EXIT_CONFIG)
        self.assertIn("non-empty listener task token file", completed.stderr)
        self.assertFalse(wrapper_record.exists())

    def test_listener_wrapper_discards_failed_bsd_stat_probe_output(self):
        _, _, wrapper_record, environment = self.listener_wrapper_fixture()
        stat_fixture = self.root / "stat"
        stat_fixture.write_text(
            """#!/bin/sh
if [ "$1" = "-f" ]; then
  printf '%s\n' 'gnu-filesystem-output'
  exit 1
fi
if [ "$1" = "-c" ]; then
  if [ "$3" = "$MERISTEM_LISTENER_CODEX_HOME" ]; then
    printf '%s\n' '700'
  else
    printf '%s\n' '600'
  fi
  exit 0
fi
exit 2
""",
            encoding="utf-8",
        )
        stat_fixture.chmod(0o700)
        environment["PATH"] = str(self.root) + os.pathsep + environment["PATH"]
        completed = subprocess.run(
            [str(WRAPPER_PATH), "app-server", "--stdio"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(wrapper_record.exists())

    def test_listener_mcp_credential_regression_script(self):
        completed = subprocess.run(
            ["bash", str(MCP_COMMAND_TEST_PATH)],
            cwd=MODULE_PATH.parent.parent,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_activation_declines_exact_mcp_elicitation_after_admission(self):
        args = self.activation_args()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args,
                self.command(
                    "trusted_mcp_approval",
                    request_method="mcpServer/elicitation/request",
                ),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
        self.assertEqual(
            [json.loads(line) for line in stdout.getvalue().splitlines()],
            [
                {"outcome": "accepted", "reason": "turn_admitted"},
                {"outcome": "failed", "reason": "authority_request_declined"},
            ],
        )

    def test_activation_declines_trusted_mcp_request_before_turn_admission(self):
        args = self.activation_args()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args,
                self.command(
                    "trusted_mcp_approval_before_admission",
                    request_method="mcpServer/elicitation/request",
                ),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
        self.assertEqual(
            [json.loads(line) for line in stdout.getvalue().splitlines()],
            [{"outcome": "failed", "reason": "authority_request_declined"}],
        )

    def test_activation_authority_decline_wins_over_failed_or_interrupted_turn(self):
        for scenario in (
            "trusted_mcp_approval_failed",
            "trusted_mcp_approval_interrupted",
        ):
            with self.subTest(scenario=scenario):
                self.record.unlink(missing_ok=True)
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    result = NUDGE.activate(
                        self.activation_args(),
                        self.command(
                            scenario,
                            request_method="mcpServer/elicitation/request",
                        ),
                        environment=self.environment,
                    )
                self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
                self.assertEqual(
                    [json.loads(line) for line in stdout.getvalue().splitlines()],
                    [
                        {"outcome": "accepted", "reason": "turn_admitted"},
                        {
                            "outcome": "failed",
                            "reason": "authority_request_declined",
                        },
                    ],
                )

    def test_activation_decline_during_resume_beats_reconciled_completion(self):
        args = self.activation_args("reconcile")
        client_id = NUDGE.activation_client_message_id(args.activation_id)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = NUDGE.activate(
                args,
                self.command(
                    "reconcile_server_request",
                    client_id,
                    request_method="mcpServer/elicitation/request",
                ),
                environment=self.environment,
            )
        self.assertEqual(result, NUDGE.EXIT_TERMINAL_FAILURE)
        self.assertEqual(
            [json.loads(line) for line in stdout.getvalue().splitlines()],
            [{"outcome": "failed", "reason": "authority_request_declined"}],
        )

    def test_mcp_approval_wrong_tool_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
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

    def test_mcp_approval_wrong_thread_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
        client.last_server_request_method = None
        client._send = mock.Mock()
        client._respond_to_server_request({
            "id": 8,
            "method": "mcpServer/elicitation/request",
            "params": {
                "serverName": "meristem_listener",
                "threadId": "019fc9ec-2d6b-7861-af0e-c1a8b540d5ff",
                "turnId": "turn-1",
                "mode": "form",
                "message": "Allow the meristem_listener MCP server to run tool \"work_items.get\"?",
                "requestedSchema": {"type": "object", "properties": {}},
            },
        })
        client._send.assert_called_once_with({
            "id": 8,
            "result": {"action": "decline", "content": None},
        })

    def test_mcp_approval_wrong_activation_turn_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
        client.last_server_request_method = None
        client._send = mock.Mock()
        client._respond_to_server_request({
            "id": 8,
            "method": "mcpServer/elicitation/request",
            "params": {
                "serverName": "meristem_listener",
                "threadId": self.thread_id,
                "turnId": "stale-turn",
                "mode": "form",
                "message": (
                    "Allow the meristem_listener MCP server to run tool "
                    '"work_items.get"?'
                ),
                "requestedSchema": {"type": "object", "properties": {}},
            },
        })
        client._send.assert_called_once_with({
            "id": 8,
            "result": {"action": "decline", "content": None},
        })

    def test_mcp_approval_structured_and_message_server_disagreement_declines(self):
        for structured_name, message_name in (
            ("meristem_listener", "other_server"),
            ("other_server", "meristem_listener"),
        ):
            with self.subTest(
                structured_name=structured_name, message_name=message_name
            ):
                client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
                client.last_server_request_method = None
                client._send = mock.Mock()
                client._respond_to_server_request({
                    "id": 9,
                    "method": "mcpServer/elicitation/request",
                    "params": {
                        "serverName": structured_name,
                        "threadId": self.thread_id,
                        "turnId": "turn-1",
                        "mode": "form",
                        "message": (
                            f"Allow the {message_name} MCP server to run tool "
                            '"work_items.get"?'
                        ),
                        "requestedSchema": {"type": "object", "properties": {}},
                    },
                })
                client._send.assert_called_once_with({
                    "id": 9,
                    "result": {"action": "decline", "content": None},
                })

    def test_mcp_approval_shape_drift_still_declines(self):
        client = NUDGE.AppServerClient.__new__(NUDGE.AppServerClient)
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
            "--listener-codex-home",
            str(self.listener_codex_home),
            "--listener-codex-sqlite-home",
            str(self.listener_codex_sqlite_home),
            "--activation-id",
            args.activation_id,
            "--work-item-id",
            args.work_item_id,
            "--assignment-event-id",
            args.assignment_event_id,
            "--task-principal-token-id",
            args.task_principal_token_id,
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

    def test_server_request_before_admission_fails_closed_without_duplicate(self):
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

    def test_all_schema_server_requests_fail_closed_after_admission(self):
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
        self.assertEqual(sanitized["CODEX_THREAD_ID"], self.thread_id)
        self.assertEqual(
            sanitized["MERISTEM_MCP_EXPECT_ACTOR_ID"],
            self.environment["MERISTEM_MCP_EXPECT_ACTOR_ID"],
        )
        self.assertNotIn("MERISTEM_TOKEN", sanitized)
        self.assertNotIn("MERISTEM_TOKEN_FILE", sanitized)
        self.assertNotIn("CODEX_MERISTEM_TOKEN_FILE", sanitized)
        self.assertNotIn("OPENAI_API_KEY", sanitized)
        self.assertNotIn("TEST_SENTINEL_SECRET", sanitized)

    def test_activation_binding_ids_must_be_exact_canonical_non_nil_uuids(self):
        for attribute, malformed in (
            ("activation_id", "00000000-0000-0000-0000-000000000000"),
            ("work_item_id", self.work_item_id.upper()),
            ("assignment_event_id", " " + self.activation_args().assignment_event_id),
            ("task_principal_token_id", "not-a-uuid"),
        ):
            with self.subTest(attribute=attribute):
                args = self.activation_args()
                setattr(args, attribute, malformed)
                with self.assertRaises(NUDGE.ConfigError):
                    NUDGE._validate_args(args)

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
