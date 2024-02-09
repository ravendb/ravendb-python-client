from __future__ import annotations
import os
import pathlib
import glob
from typing import TYPE_CHECKING

from ravendb_embedded.embedded_server import EmbeddedServer
from ravendb_embedded.options import ServerOptions, SecurityOptions
from ravendb_embedded.provide import ExternalServerProvider

from ravendb.tests.driver.raven_server_locator import RavenServerLocator

if TYPE_CHECKING:
    from ravendb.tests.test_base import TestBase


class RavenServerRunner:
    @staticmethod  # workaround for places where we cannot import TestBase.TestSecuredServiceLocator
    def is_locator_secured(locator: RavenServerLocator) -> bool:
        return hasattr(locator, "client_certificate_path")

    @staticmethod
    def get_server_options_for_embedded_server(
        locator: RavenServerLocator, server_dll_parent_dir: str
    ) -> ServerOptions:
        process_start_info = RavenServerRunner.get_process_start_info(locator)
        server_options = ServerOptions()
        server_options.server_url = "http://127.0.0.1:0"
        server_options.command_line_args = ["--Features.Availability=Experimental"]
        server_options.command_line_args.extend(process_start_info.arguments)
        server_options.provider = ExternalServerProvider(server_dll_parent_dir)
        server_options.target_server_location = server_dll_parent_dir

        if RavenServerRunner.is_locator_secured(locator):
            locator: "TestBase.TestSecuredServiceLocator"
            server_options.security = SecurityOptions()
            server_options.security.server_pfx_certificate_path = locator.server_certificate_path
            server_options.security.client_pem_certificate_path = locator.client_certificate_path
            server_options.security.ca_certificate_path = locator.server_ca_path
            server_options.server_url = locator.https_server_url

        return server_options

    @staticmethod
    def get_embedded_server(locator: RavenServerLocator) -> EmbeddedServer:
        raven_server_dll_pardir = pathlib.Path(locator.get_server_path()).parent.__str__()
        server_options = RavenServerRunner.get_server_options_for_embedded_server(locator, raven_server_dll_pardir)

        embedded = EmbeddedServer()
        embedded.start_server(server_options)

        return embedded

    @staticmethod
    def get_process_start_info(locator: RavenServerLocator) -> _ProcessStartInfo:
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

        process_start_info = RavenServerRunner._ProcessStartInfo(locator.command, *command_arguments)

        return process_start_info

    @staticmethod
    def get_process_id(fallback: str) -> str:
        try:  # should always work
            return str(os.getpid())
        except Exception:
            return fallback

    class _ProcessStartInfo:
        def __init__(self, command: str, *arguments: str):
            self.command = command
            self.arguments = arguments
