from __future__ import annotations

import os
import pathlib
import subprocess
from ravendb.tests.driver.raven_server_locator import RavenServerLocator


class RavenServerRunner:
    @staticmethod
    def run(locator: RavenServerLocator) -> subprocess.Popen:
        process_start_info = RavenServerRunner.get_process_start_info(locator)

        arguments = [process_start_info.command]
        arguments.extend(process_start_info.arguments)

        return subprocess.Popen(arguments, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    @staticmethod
    def get_process_start_info(locator: RavenServerLocator) -> __ProcessStartInfo:
        server_path = pathlib.Path(locator.get_server_path())
        if not server_path.exists():
            raise FileNotFoundError(f"Server file was not found: {locator.get_server_path()}")

        command_arguments = [
            "--RunInMemory=true",
            "--License.Eula.Accepted=true",
            "--Setup.Mode=None",
            "--Logs.Mode=None",
            f"--Testing.ParentProcessId={RavenServerRunner.get_process_id('0')}",
        ]

        command_arguments.extend(locator.command_arguments)

        process_start_info = RavenServerRunner.__ProcessStartInfo(locator.command, *command_arguments)

        return process_start_info

    @staticmethod
    def get_process_id(fallback: str) -> str:
        try:  # should always work
            return str(os.getpid())
        except Exception:
            return fallback

    class __ProcessStartInfo:
        def __init__(self, command: str, *arguments: str):
            self.command = command
            self.arguments = arguments
