from __future__ import annotations
from abc import ABC
from typing import TYPE_CHECKING, Optional, List

from ravendb.documents.operations.etl.configuration import EtlConfiguration
from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiConnectorType
from ravendb.documents.operations.etl.transformation import Transformation

if TYPE_CHECKING:
    pass


class AbstractAiIntegrationConfiguration(EtlConfiguration[AiConnectionString], ABC):
    """
    Base class for AI integration configurations.
    Extends EtlConfiguration with AiConnectionString as the connection type.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        task_id: int = 0,
        connection_string_name: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: bool = False,
        transforms: Optional[List[Transformation]] = None,
        disabled: bool = False,
        allow_etl_on_non_encrypted_channel: bool = False,
    ):
        super().__init__(
            name=name,
            task_id=task_id,
            connection_string_name=connection_string_name,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
            transforms=transforms,
            disabled=disabled,
            allow_etl_on_non_encrypted_channel=allow_etl_on_non_encrypted_channel,
        )

    @property
    def ai_connector_type(self) -> AiConnectorType:
        """Returns the AI connector type based on the active provider in the connection."""
        if self.connection:
            return self.connection.get_active_provider()
        return AiConnectorType.NONE
