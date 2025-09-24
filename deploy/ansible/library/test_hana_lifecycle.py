import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import subprocess

# Add the current directory to Python path to import the module being tested
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the module being tested
from hana_lifecycle import HANALifecycleManager, main


class TestHANALifecycleManager:
    """Test class for HANA lifecycle operations."""

    def setup_method(self):
        """Setup method called before each test method."""
        # Create a mock AnsibleModule
        self.mock_module = Mock()
        self.mock_module.params = {
            "sid": "HDB",
            "instance_number": "00",
            "operation": "status",
            "timeout": 300,
            "sapcontrol_path": "/usr/sap/hostctrl/exe/sapcontrol",
        }
        self.mock_module.fail_json = Mock()
        self.mock_module.exit_json = Mock()

    def teardown_method(self):
        """Teardown method called after each test method."""
        pass

    @patch("os.path.isfile")
    def test_init_with_valid_sapcontrol_path(self, mock_isfile):
        """Test HANALifecycleManager initialization with valid sapcontrol path."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        assert manager.sid == "HDB"
        assert manager.instance_number == "00"
        assert manager.timeout == 300
        assert manager.sapcontrol_path == "/usr/sap/hostctrl/exe/sapcontrol"

    @patch("os.path.isfile")
    def test_init_with_invalid_sapcontrol_path_finds_alternative(self, mock_isfile):
        """Test HANALifecycleManager initialization finds alternative sapcontrol path."""
        # First path fails, second path succeeds
        mock_isfile.side_effect = [False, True]

        manager = HANALifecycleManager(self.mock_module)

        assert (
            manager.sapcontrol_path == "/usr/sap/HDB/SYS/exe/uc/linuxx86_64/sapcontrol"
        )

    @patch("os.path.isfile")
    def test_init_with_no_valid_sapcontrol_path(self, mock_isfile):
        """Test HANALifecycleManager initialization fails when no sapcontrol found."""
        mock_isfile.return_value = False

        HANALifecycleManager(self.mock_module)

        self.mock_module.fail_json.assert_called_once()
        call_args = self.mock_module.fail_json.call_args[1]
        assert "sapcontrol executable not found" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_run_sapcontrol_command_success(self, mock_run, mock_isfile):
        """Test successful execution of sapcontrol command."""
        mock_isfile.return_value = True
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "success output"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        manager = HANALifecycleManager(self.mock_module)
        result = manager._run_sapcontrol_command(["-function", "GetProcessList"])

        assert result["rc"] == 0
        assert result["stdout"] == "success output"
        assert result["stderr"] == ""

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_run_sapcontrol_command_timeout(self, mock_run, mock_isfile):
        """Test sapcontrol command timeout handling."""
        mock_isfile.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(["cmd"], 300)

        manager = HANALifecycleManager(self.mock_module)
        manager._run_sapcontrol_command(["-function", "GetProcessList"])

        self.mock_module.fail_json.assert_called_once()
        call_args = self.mock_module.fail_json.call_args[1]
        assert "timed out" in call_args["msg"]

    @patch("os.path.isfile")
    def test_get_instance_status_all_stopped(self, mock_isfile):
        """Test get_instance_status when all processes are stopped."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "_run_sapcontrol_command") as mock_cmd:
            mock_cmd.return_value = {
                "rc": 4,  # All stopped
                "stdout": "All processes stopped",
                "stderr": "",
            }

            result = manager.get_instance_status()

            assert result["status"] == "STOPPED"
            assert result["processes"] == []

    @patch("os.path.isfile")
    def test_get_instance_status_all_running(self, mock_isfile):
        """Test get_instance_status when all processes are running."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        stdout_output = """name, description, dispstatus, textstatus, starttime, elapsedtime, pid
hdbdaemon, HDB Daemon, GREEN, Running, 2024-01-01 10:00:00, 1:00:00, 1234
hdbcompileserver, HDB Compile Server, GREEN, Running, 2024-01-01 10:01:00, 0:59:00, 1235"""

        with patch.object(manager, "_run_sapcontrol_command") as mock_cmd:
            mock_cmd.return_value = {
                "rc": 3,  # All running
                "stdout": stdout_output,
                "stderr": "",
            }

            result = manager.get_instance_status()

            assert result["status"] == "GREEN"
            assert len(result["processes"]) == 2
            assert result["processes"][0]["name"] == "hdbdaemon"
            assert result["processes"][0]["status"] == "GREEN"

    @patch("os.path.isfile")
    def test_get_instance_status_mixed_states(self, mock_isfile):
        """Test get_instance_status with mixed process states."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        stdout_output = """name, description, dispstatus, textstatus, starttime, elapsedtime, pid
hdbdaemon, HDB Daemon, GREEN, Running, 2024-01-01 10:00:00, 1:00:00, 1234
hdbcompileserver, HDB Compile Server, YELLOW, Starting, 2024-01-01 10:01:00, 0:59:00, 1235
hdbindexserver, HDB Index Server, RED, Stopped, , , """

        with patch.object(manager, "_run_sapcontrol_command") as mock_cmd:
            mock_cmd.return_value = {"rc": 0, "stdout": stdout_output, "stderr": ""}

            result = manager.get_instance_status()

            assert result["status"] == "RED"  # Worst case
            assert len(result["processes"]) == 3

    @patch("os.path.isfile")
    @patch("time.sleep")
    def test_start_instance_already_running(self, mock_sleep, mock_isfile):
        """Test start_instance when instance is already running."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            result = manager.start_instance()

            assert result["changed"] is False
            assert "already running" in result["msg"]
            assert result["status"] == "GREEN"

    @patch("os.path.isfile")
    @patch("time.sleep")
    @patch("time.time")
    def test_start_instance_successful(self, mock_time, mock_sleep, mock_isfile):
        """Test successful start_instance operation."""
        mock_isfile.return_value = True
        mock_time.side_effect = [0, 15]  # Start time and check time

        manager = HANALifecycleManager(self.mock_module)

        status_calls = [
            {"status": "STOPPED", "processes": []},  # Initial status
            {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            },  # After start
        ]

        with patch.object(
            manager, "get_instance_status", side_effect=status_calls
        ), patch.object(manager, "_run_sapcontrol_command") as mock_cmd:

            mock_cmd.return_value = {"rc": 0, "stdout": "Start initiated", "stderr": ""}

            result = manager.start_instance()

            assert result["changed"] is True
            assert "started successfully" in result["msg"]
            assert result["status"] == "GREEN"

    @patch("os.path.isfile")
    @patch("time.sleep")
    def test_stop_instance_already_stopped(self, mock_sleep, mock_isfile):
        """Test stop_instance when instance is already stopped."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {"status": "STOPPED", "processes": []}

            result = manager.stop_instance()

            assert result["changed"] is False
            assert "already stopped" in result["msg"]
            assert result["status"] == "STOPPED"

    @patch("os.path.isfile")
    @patch("time.sleep")
    @patch("time.time")
    def test_stop_instance_successful(self, mock_time, mock_sleep, mock_isfile):
        """Test successful stop_instance operation."""
        mock_isfile.return_value = True
        mock_time.side_effect = [0, 10]  # Start time and check time

        manager = HANALifecycleManager(self.mock_module)

        status_calls = [
            {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            },  # Initial status
            {"status": "STOPPED", "processes": []},  # After stop
        ]

        with patch.object(
            manager, "get_instance_status", side_effect=status_calls
        ), patch.object(manager, "_run_sapcontrol_command") as mock_cmd:

            mock_cmd.return_value = {"rc": 0, "stdout": "Stop initiated", "stderr": ""}

            result = manager.stop_instance()

            assert result["changed"] is True
            assert "stopped successfully" in result["msg"]
            assert result["status"] == "STOPPED"

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_status_primary(self, mock_run, mock_isfile):
        """Test check_replication_status for primary site."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        # Mock hdbnsutil output for primary site
        hdbnsutil_output = """checking for active or inactive nameserver ...
mode: primary
site id: 1
site name: SITE1
online: true
active primary site: 1
primary masters: hanadb1"""

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = hdbnsutil_output
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            result = manager.check_replication_status()

            assert result["changed"] is False
            assert result["status"] == "GREEN"
            assert result["replication_status"]["mode"] == "primary"
            assert result["replication_status"]["site_name"] == "SITE1"
            assert result["replication_status"]["is_primary"] is True
            assert result["replication_status"]["is_secondary"] is False

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_status_secondary(self, mock_run, mock_isfile):
        """Test check_replication_status for secondary site."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        # Mock hdbnsutil output for secondary site
        hdbnsutil_output = """checking for active or inactive nameserver ...
mode: sync
site id: 2
site name: SITE2
online: true
active primary site: 1
primary masters: hanadb1"""

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = hdbnsutil_output
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            result = manager.check_replication_status()

            assert result["replication_status"]["mode"] == "sync"
            assert result["replication_status"]["site_name"] == "SITE2"
            assert result["replication_status"]["is_primary"] is False
            assert result["replication_status"]["is_secondary"] is True

    @patch("os.path.isfile")
    def test_check_replication_status_instance_not_running(self, mock_isfile):
        """Test check_replication_status when HANA instance is not running."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {"status": "STOPPED", "processes": []}

            result = manager.check_replication_status()

            assert result["changed"] is False
            assert "must be running" in result["msg"]
            assert result["replication_status"] is None

    @pytest.mark.parametrize(
        "operation,expected_method",
        [
            ("status", "get_instance_status"),
            ("start", "start_instance"),
            ("stop", "stop_instance"),
            ("check_replication", "check_replication_status"),
        ],
    )
    @patch("hana_lifecycle.AnsibleModule")
    @patch("hana_lifecycle.HANALifecycleManager")
    def test_main_function_operations(
        self, mock_manager_class, mock_ansible_module, operation, expected_method
    ):
        """Test main function delegates to correct manager methods."""
        # Setup mocks
        mock_module = Mock()
        mock_module.params = {
            "operation": operation,
            "sid": "HDB",
            "instance_number": "00",
            "timeout": 300,
            "sapcontrol_path": "/usr/sap/hostctrl/exe/sapcontrol",
        }
        mock_module.check_mode = False
        mock_ansible_module.return_value = mock_module

        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager

        # Configure method return values
        if expected_method == "get_instance_status":
            getattr(mock_manager, expected_method).return_value = {
                "status": "GREEN",
                "processes": [],
            }
        else:
            getattr(mock_manager, expected_method).return_value = {
                "changed": True,
                "msg": "Success",
            }

        # Call main
        main()

        # Verify correct method was called
        getattr(mock_manager, expected_method).assert_called_once()
        mock_module.exit_json.assert_called_once()

    @patch("hana_lifecycle.AnsibleModule")
    @patch("hana_lifecycle.HANALifecycleManager")
    def test_main_function_check_mode_start(
        self, mock_manager_class, mock_ansible_module
    ):
        """Test main function in check mode for start operation."""
        mock_module = Mock()
        mock_module.params = {
            "operation": "start",
            "sid": "HDB",
            "instance_number": "00",
            "timeout": 300,
            "sapcontrol_path": "/usr/sap/hostctrl/exe/sapcontrol",
        }
        mock_module.check_mode = True
        mock_ansible_module.return_value = mock_module

        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.get_instance_status.return_value = {
            "status": "STOPPED",
            "processes": [],
        }

        main()

        # Verify get_instance_status was called instead of start_instance
        mock_manager.get_instance_status.assert_called_once()
        mock_manager.start_instance.assert_not_called()
        mock_module.exit_json.assert_called_once()

    @patch("hana_lifecycle.AnsibleModule")
    @patch("hana_lifecycle.HANALifecycleManager")
    def test_main_function_check_mode_stop(
        self, mock_manager_class, mock_ansible_module
    ):
        """Test main function in check mode for stop operation."""
        mock_module = Mock()
        mock_module.params = {
            "operation": "stop",
            "sid": "HDB",
            "instance_number": "00",
            "timeout": 300,
            "sapcontrol_path": "/usr/sap/hostctrl/exe/sapcontrol",
        }
        mock_module.check_mode = True
        mock_ansible_module.return_value = mock_module

        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.get_instance_status.return_value = {
            "status": "GREEN",
            "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
        }

        main()

        mock_manager.get_instance_status.assert_called_once()
        mock_manager.stop_instance.assert_not_called()
        mock_module.exit_json.assert_called_once()

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_run_sapcontrol_command_exception(self, mock_run, mock_isfile):
        """Test sapcontrol command exception handling."""
        mock_isfile.return_value = True
        mock_run.side_effect = Exception("Subprocess error")

        manager = HANALifecycleManager(self.mock_module)
        manager._run_sapcontrol_command(["-function", "GetProcessList"])

        self.mock_module.fail_json.assert_called_once()
        call_args = self.mock_module.fail_json.call_args[1]
        assert "Failed to execute sapcontrol command" in call_args["msg"]

    @patch("os.path.isfile")
    def test_get_instance_status_none_result(self, mock_isfile):
        """Test get_instance_status when command returns None."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "_run_sapcontrol_command") as mock_cmd:
            mock_cmd.return_value = None

            result = manager.get_instance_status()

            assert result["status"] == "UNKNOWN"
            assert result["processes"] == []

    @patch("os.path.isfile")
    def test_get_instance_status_invalid_return_code(self, mock_isfile):
        """Test get_instance_status with invalid return code."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "_run_sapcontrol_command") as mock_cmd:
            mock_cmd.return_value = {"rc": 1, "stdout": "", "stderr": "Error"}

            result = manager.get_instance_status()

            assert result["status"] == "UNKNOWN"
            assert result["processes"] == []
            assert result["raw_output"] == "Error"

    @patch("os.path.isfile")
    @patch("time.sleep")
    @patch("time.time")
    def test_start_instance_red_status_failure(
        self, mock_time, mock_sleep, mock_isfile
    ):
        """Test start_instance fails when status becomes RED."""
        mock_isfile.return_value = True
        # Mock time to always return 0 (so we never timeout)
        mock_time.return_value = 0

        manager = HANALifecycleManager(self.mock_module)

        call_count = 0

        def mock_get_status():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"status": "STOPPED", "processes": []}  # Initial status
            else:
                return {
                    "status": "RED",
                    "processes": [{"name": "hdbdaemon", "status": "RED"}],
                }  # After start

        with patch.object(
            manager, "get_instance_status", side_effect=mock_get_status
        ), patch.object(manager, "_run_sapcontrol_command") as mock_cmd:

            mock_cmd.return_value = {"rc": 0, "stdout": "Start initiated", "stderr": ""}

            manager.start_instance()

            self.mock_module.fail_json.assert_called_once()
            call_args = self.mock_module.fail_json.call_args[1]
            assert "failed to start properly - status is RED" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("time.sleep")
    @patch("time.time")
    def test_start_instance_timeout(self, mock_time, mock_sleep, mock_isfile):
        """Test start_instance timeout scenario."""
        mock_isfile.return_value = True
        # Simulate timeout by time progressing beyond timeout limit
        mock_time.side_effect = [0, 350]  # Beyond 300 second timeout

        manager = HANALifecycleManager(self.mock_module)

        status_calls = [
            {"status": "STOPPED", "processes": []},  # Initial status
            {
                "status": "YELLOW",
                "processes": [{"name": "hdbdaemon", "status": "YELLOW"}],
            },  # Still starting
        ]

        with patch.object(
            manager, "get_instance_status", side_effect=status_calls
        ), patch.object(manager, "_run_sapcontrol_command") as mock_cmd:

            mock_cmd.return_value = {"rc": 0, "stdout": "Start initiated", "stderr": ""}

            manager.start_instance()

            self.mock_module.fail_json.assert_called()
            call_args = self.mock_module.fail_json.call_args[1]
            assert "startup timed out" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("time.sleep")
    @patch("time.time")
    def test_stop_instance_timeout(self, mock_time, mock_sleep, mock_isfile):
        """Test stop_instance timeout scenario."""
        mock_isfile.return_value = True
        # Simulate timeout
        mock_time.side_effect = [0, 350]  # Beyond 300 second timeout

        manager = HANALifecycleManager(self.mock_module)

        status_calls = [
            {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            },  # Initial status
            {
                "status": "YELLOW",
                "processes": [{"name": "hdbdaemon", "status": "YELLOW"}],
            },  # Still stopping
        ]

        with patch.object(
            manager, "get_instance_status", side_effect=status_calls
        ), patch.object(manager, "_run_sapcontrol_command") as mock_cmd:

            mock_cmd.return_value = {"rc": 0, "stdout": "Stop initiated", "stderr": ""}

            manager.stop_instance()

            self.mock_module.fail_json.assert_called()
            call_args = self.mock_module.fail_json.call_args[1]
            assert "shutdown timed out" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_hdbnsutil_timeout(self, mock_run, mock_isfile):
        """Test check_replication_status with hdbnsutil timeout."""
        mock_isfile.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(["cmd"], 300)

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            manager.check_replication_status()

            self.mock_module.fail_json.assert_called_once()
            call_args = self.mock_module.fail_json.call_args[1]
            assert "hdbnsutil command timed out" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_hdbnsutil_exception(self, mock_run, mock_isfile):
        """Test check_replication_status with hdbnsutil exception."""
        mock_isfile.return_value = True
        mock_run.side_effect = Exception("Subprocess error")

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            manager.check_replication_status()

            self.mock_module.fail_json.assert_called_once()
            call_args = self.mock_module.fail_json.call_args[1]
            assert "Failed to execute hdbnsutil command" in call_args["msg"]

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_status_disabled(self, mock_run, mock_isfile):
        """Test check_replication_status for disabled replication (mode: none)."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        # Mock hdbnsutil output for disabled replication
        hdbnsutil_output = """checking for active or inactive nameserver ...
mode: none
site id: 1
site name: SITE1
online: false"""

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = hdbnsutil_output
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            result = manager.check_replication_status()

            assert result["replication_status"]["mode"] == "DISABLED"
            assert result["replication_status"]["is_primary"] is False
            assert result["replication_status"]["is_secondary"] is False

    @patch("os.path.isfile")
    @patch("subprocess.run")
    def test_check_replication_status_parsing_errors(self, mock_run, mock_isfile):
        """Test check_replication_status with parsing errors in output."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        # Mock hdbnsutil output with invalid values
        hdbnsutil_output = """checking for active or inactive nameserver ...
mode: sync
site id: invalid_number
site name: SITE2
online: true
active primary site: also_invalid"""

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = hdbnsutil_output
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        with patch.object(manager, "get_instance_status") as mock_status:
            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }

            result = manager.check_replication_status()

            # Should handle parsing errors gracefully
            assert result["replication_status"]["site_id"] is None
            assert result["replication_status"]["active_primary_site"] is None
            assert result["replication_status"]["site_name"] == "SITE2"

    @patch("hana_lifecycle.AnsibleModule")
    @patch("hana_lifecycle.HANALifecycleManager")
    def test_main_function_exception_handling(
        self, mock_manager_class, mock_ansible_module
    ):
        """Test main function exception handling."""
        mock_module = Mock()
        mock_module.params = {
            "operation": "status",
            "sid": "HDB",
            "instance_number": "00",
            "timeout": 300,
            "sapcontrol_path": "/usr/sap/hostctrl/exe/sapcontrol",
        }
        mock_module.check_mode = False
        mock_ansible_module.return_value = mock_module

        # Make the manager method raise an exception instead of initialization
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.get_instance_status.side_effect = Exception("Test exception")

        main()

        mock_module.fail_json.assert_called_once()
        call_args = mock_module.fail_json.call_args[1]
        assert "Unexpected error" in call_args["msg"]

    @patch("os.path.isfile")
    def test_start_instance_command_failure(self, mock_isfile):
        """Test start_instance when sapcontrol start command fails."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status, patch.object(
            manager, "_run_sapcontrol_command"
        ) as mock_cmd:

            mock_status.return_value = {"status": "STOPPED", "processes": []}
            mock_cmd.return_value = {"rc": 1, "stdout": "", "stderr": "Start failed"}

            manager.start_instance()

            # Should be called exactly once for command failure, not for timeout
            assert self.mock_module.fail_json.call_count >= 1
            # Check the first call was for the command failure
            first_call_args = self.mock_module.fail_json.call_args_list[0][1]
            assert "Failed to start HANA instance" in first_call_args["msg"]

    @patch("os.path.isfile")
    def test_stop_instance_command_failure(self, mock_isfile):
        """Test stop_instance when sapcontrol stop command fails."""
        mock_isfile.return_value = True

        manager = HANALifecycleManager(self.mock_module)

        with patch.object(manager, "get_instance_status") as mock_status, patch.object(
            manager, "_run_sapcontrol_command"
        ) as mock_cmd:

            mock_status.return_value = {
                "status": "GREEN",
                "processes": [{"name": "hdbdaemon", "status": "GREEN"}],
            }
            mock_cmd.return_value = {"rc": 1, "stdout": "", "stderr": "Stop failed"}

            manager.stop_instance()

            # Should be called at least once for command failure
            assert self.mock_module.fail_json.call_count >= 1
            # Check the first call was for the command failure
            first_call_args = self.mock_module.fail_json.call_args_list[0][1]
            assert "Failed to stop HANA instance" in first_call_args["msg"]


if __name__ == "__main__":
    pytest.main([__file__])
