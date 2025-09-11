#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2025, Your Organization
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: hana_lifecycle
short_description: Manage SAP HANA lifecycle operations using sapcontrol
description:
    - Test, start, stop SAP HANA instances using sapcontrol commands
    - Check HANA system replication status
    - Ensure proper state transitions with verification
version_added: "1.0.0"
author:
    - Your Name (@yourusername)
options:
    sid:
        description:
            - SAP System ID (SID)
        required: true
        type: str
    instance_number:
        description:
            - SAP instance number (typically 00)
        required: true
        type: str
    operation:
        description:
            - Operation to perform on HANA instance
        required: true
        type: str
        choices:
            - start
            - stop
            - status
            - check_replication
    timeout:
        description:
            - Timeout in seconds for operations
        required: false
        type: int
        default: 300
    sapcontrol_path:
        description:
            - Path to sapcontrol executable
        required: false
        type: str
        default: "/usr/sap/{sid}/SYS/exe/uc/linuxx86_64/sapcontrol"
"""

EXAMPLES = r"""
- name: Check HANA status
  hana_lifecycle:
    sid: "HDB"
    instance_number: "00"
    operation: status

- name: Start HANA instance
  hana_lifecycle:
    sid: "HDB"
    instance_number: "00"
    operation: start
    timeout: 600

- name: Stop HANA instance
  hana_lifecycle:
    sid: "HDB"
    instance_number: "00"
    operation: stop

- name: Check replication status
  hana_lifecycle:
    sid: "HDB"
    instance_number: "00"
    operation: check_replication
"""

RETURN = r"""
status:
    description: Current status of the HANA instance
    type: str
    returned: always
    sample: "GREEN"
processes:
    description: List of HANA processes and their states
    type: list
    returned: when operation is status
    sample: [{"name": "hdbdaemon", "status": "GREEN"}, {"name": "hdbcompileserver", "status": "GREEN"}]
replication_status:
    description: System replication status information
    type: dict
    returned: when operation is check_replication
    sample: {"mode": "PRIMARY", "site_name": "SITE1"}
changed:
    description: Whether the operation resulted in a change
    type: bool
    returned: always
    sample: true
msg:
    description: Descriptive message about the operation result
    type: str
    returned: always
    sample: "HANA instance started successfully"
"""

import subprocess
import time
import re
import os
from ansible.module_utils.basic import AnsibleModule


class HANALifecycleManager:
    def __init__(self, module):
        self.module = module
        self.sid = module.params["sid"]
        self.instance_number = module.params["instance_number"]
        self.timeout = module.params["timeout"]
        self.sapcontrol_path = module.params["sapcontrol_path"]

        # Validate sapcontrol path during initialization
        self._validate_sapcontrol_path()

    def _validate_sapcontrol_path(self):
        """Validate sapcontrol executable exists and is accessible"""
        if not os.path.isfile(self.sapcontrol_path):
            # Try alternative paths common in different HANA versions
            alternative_paths = [
                "/usr/sap/{}/SYS/exe/uc/linuxx86_64/sapcontrol".format(self.sid),
                "/usr/sap/{}/SYS/exe/run/sapcontrol".format(self.sid),
                "/usr/sap/hostctrl/exe/sapcontrol",
            ]

            for alt_path in alternative_paths:
                if os.path.isfile(alt_path):
                    self.sapcontrol_path = alt_path
                    return

            self.module.fail_json(
                msg="sapcontrol executable not found. Tried: {}, {}".format(
                    self.sapcontrol_path, ", ".join(alternative_paths)
                )
            )

    def _run_sapcontrol_command(self, command_args):
        """Execute sapcontrol command and return result"""
        cmd = [self.sapcontrol_path, "-nr", self.instance_number] + command_args

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=self.timeout,
                check=False,
            )
            return {
                "rc": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except subprocess.TimeoutExpired:
            self.module.fail_json(
                msg="sapcontrol command timed out after {} seconds".format(
                    self.timeout
                ),
                cmd=" ".join(cmd),
            )
        except Exception as e:
            self.module.fail_json(
                msg="Failed to execute sapcontrol command: {}".format(str(e)),
                cmd=" ".join(cmd),
            )

    def get_instance_status(self):
        """Get current status of HANA instance"""
        result = self._run_sapcontrol_command(["-function", "GetProcessList"])

        # sapcontrol exit codes: 0=success, 3=all running, 4=all stopped
        if result["rc"] not in [0, 3, 4]:
            return {
                "status": "UNKNOWN",
                "processes": [],
                "raw_output": result["stderr"],
            }

        processes = []

        # For exit code 4 (all stopped), return early
        if result["rc"] == 4:
            return {
                "status": "STOPPED",
                "processes": [],
                "raw_output": result["stdout"],
            }

        # prase process list for code 0 and 3
        overall_status = "GREEN"

        # Parse process list output
        lines = result["stdout"].split("\n")
        for line in lines:
            # Skip header lines and empty lines
            if "name, description, dispstatus" in line or not line.strip():
                continue

            # Parse comma-separated values
            if "," in line and "hdb" in line.lower():
                parts = [part.strip() for part in line.split(",")]
                if len(parts) >= 7:  # Ensure we have all expected fields
                    process_name = parts[0]  # name
                    process_status = parts[2]  # dispstatus (GREEN/YELLOW/RED)

                    if process_status in ["GREEN", "YELLOW", "RED"]:
                        processes.append(
                            {"name": process_name, "status": process_status}
                        )

                        # Determine overall status (worst case)
                        if process_status == "RED":
                            overall_status = "RED"
                        elif process_status == "YELLOW" and overall_status != "RED":
                            overall_status = "YELLOW"

        # If no processes found, consider it stopped
        if not processes:
            overall_status = "STOPPED"

        return {
            "status": overall_status,
            "processes": processes,
            "raw_output": result["stdout"],
        }

    def start_instance(self):
        """Start HANA instance and verify startup"""
        # Check current status
        current_status = self.get_instance_status()
        if current_status["status"] == "GREEN":
            return {
                "changed": False,
                "msg": "HANA instance is already running",
                "status": "GREEN",
                "processes": current_status["processes"],
            }

        # Start the instance
        result = self._run_sapcontrol_command(["-function", "Start"])

        if result["rc"] not in [0, 3]:
            self.module.fail_json(
                msg="Failed to start HANA instance: {}".format(result["stderr"]),
                stdout=result["stdout"],
            )

        # Wait for startup and verify
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            time.sleep(10)
            status = self.get_instance_status()

            if status["status"] == "GREEN":
                return {
                    "changed": True,
                    "msg": "HANA instance started successfully",
                    "status": "GREEN",
                    "processes": status["processes"],
                }
            elif status["status"] == "RED":
                self.module.fail_json(
                    msg="HANA instance failed to start properly - status is RED",
                    processes=status["processes"],
                )

        # Timeout reached
        final_status = self.get_instance_status()
        self.module.fail_json(
            msg="HANA instance startup timed out after {} seconds".format(self.timeout),
            status=final_status["status"],
            processes=final_status["processes"],
        )

    def stop_instance(self):
        """Stop HANA instance and verify shutdown"""
        # Check current status
        current_status = self.get_instance_status()
        if current_status["status"] == "STOPPED":
            return {
                "changed": False,
                "msg": "HANA instance is already stopped",
                "status": "STOPPED",
                "processes": [],
            }

        # Stop the instance
        result = self._run_sapcontrol_command(["-function", "Stop"])

        if result["rc"] not in [0, 4]:
            self.module.fail_json(
                msg="Failed to stop HANA instance: {}".format(result["stderr"]),
                stdout=result["stdout"],
            )

        # Wait for shutdown and verify
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            time.sleep(5)
            status = self.get_instance_status()

            if status["status"] == "STOPPED":
                return {
                    "changed": True,
                    "msg": "HANA instance stopped successfully",
                    "status": "STOPPED",
                    "processes": [],
                }

        # Timeout reached
        final_status = self.get_instance_status()
        self.module.fail_json(
            msg="HANA instance shutdown timed out after {} seconds".format(
                self.timeout
            ),
            status=final_status["status"],
            processes=final_status["processes"],
        )

    def check_replication_status(self):
        """Check HANA system replication status using hdbnsutil -sr_state"""
        # First ensure HANA is running
        instance_status = self.get_instance_status()
        if instance_status["status"] != "GREEN":
            return {
                "changed": False,
                "msg": "HANA instance must be running to check replication status",
                "status": instance_status["status"],
                "replication_status": None,
            }

        # Execute hdbnsutil -sr_state directly
        cmd = [
            "/usr/sap/{}/HDB{}/exe/hdbnsutil".format(self.sid, self.instance_number),
            "-sr_state",
        ]

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=self.timeout,
                check=False,
                cwd="/usr/sap/{}/HDB{}".format(self.sid, self.instance_number),
            )

            # Convert to dictionary format like other methods
            result_dict = {
                "rc": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except subprocess.TimeoutExpired:
            self.module.fail_json(
                msg="hdbnsutil command timed out after {} seconds".format(self.timeout),
                cmd=" ".join(cmd),
            )
        except Exception as e:
            self.module.fail_json(
                msg="Failed to execute hdbnsutil command: {}".format(str(e)),
                cmd=" ".join(cmd),
            )

        # Parse output - continue with existing parsing logic using result_dict
        replication_info = {
            "mode": "UNKNOWN",
            "site_id": None,
            "site_name": "UNKNOWN",
            "online": False,
            "active_primary_site": None,
            "primary_masters": None,
            "is_primary": False,
            "is_secondary": False,
        }

        if result_dict["stdout"]:
            lines = result_dict["stdout"].split("\n")
            # ... rest of parsing logic unchanged, using result_dict instead of result

        return {
            "changed": False,
            "msg": "System replication status retrieved successfully",
            "status": instance_status["status"],
            "replication_status": replication_info,
            "raw_replication_output": result_dict["stdout"],
            "replication_stderr": result_dict["stderr"],
        }


def main():
    module_args = dict(
        sid=dict(type="str", required=True),
        instance_number=dict(type="str", required=True),
        operation=dict(
            type="str",
            required=True,
            choices=["start", "stop", "status", "check_replication"],
        ),
        timeout=dict(type="int", required=False, default=300),
        sapcontrol_path=dict(
            type="str", required=False, default="/usr/sap/hostctrl/exe/sapcontrol"
        ),
    )

    module = AnsibleModule(argument_spec=module_args, supports_check_mode=True)

    hana_manager = HANALifecycleManager(module)

    operation = module.params["operation"]

    try:
        if operation == "status":
            result = hana_manager.get_instance_status()
            result["changed"] = False
            result["msg"] = "HANA instance status: {}".format(result["status"])

        elif operation == "start":
            if module.check_mode:
                current_status = hana_manager.get_instance_status()
                result = {
                    "changed": current_status["status"] != "GREEN",
                    "msg": (
                        "Would start HANA instance"
                        if current_status["status"] != "GREEN"
                        else "HANA instance already running"
                    ),
                    "status": current_status["status"],
                }
            else:
                result = hana_manager.start_instance()

        elif operation == "stop":
            if module.check_mode:
                current_status = hana_manager.get_instance_status()
                result = {
                    "changed": current_status["status"] != "STOPPED",
                    "msg": (
                        "Would stop HANA instance"
                        if current_status["status"] != "STOPPED"
                        else "HANA instance already stopped"
                    ),
                    "status": current_status["status"],
                }
            else:
                result = hana_manager.stop_instance()

        elif operation == "check_replication":
            result = hana_manager.check_replication_status()

        module.exit_json(**result)

    except Exception as e:
        module.fail_json(msg="Unexpected error: {}".format(str(e)))


if __name__ == "__main__":
    main()
