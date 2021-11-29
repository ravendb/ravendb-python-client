from typing import Union, List

from pyravendb.documents.session.tokens.query_token import QueryToken


class FieldsToFetchToken(QueryToken):
    def __init__(
        self, fields_to_fetch: List[str], projections: Union[List[str], None], custom_function: bool, source_alias: str
    ):
        self.fields_to_fetch = fields_to_fetch
        self.projections = projections
        self.custom_function = custom_function
        self.source_aliast = source_alias

    @staticmethod
    def create(fields_to_fetch: List[str], projections: List[str], custom_function: bool, source_alias=None):
        if not fields_to_fetch:
            raise ValueError("fields_to_fetch cannot be None")
        if (not custom_function) and projections is not None and len(projections) != len(fields_to_fetch):
            raise ValueError("Length of projections must be the same as length of field to fetch")

        return FieldsToFetchToken(fields_to_fetch, projections, custom_function, source_alias)

    def write_to(self, writer: List[str]):
        for i in range(len(self.fields_to_fetch)):
            field_to_fetch = self.fields_to_fetch[i]
            if i > 0:
                writer.append(" ,")
            if not field_to_fetch:
                writer.append("null")
            else:
                super().write_field(writer, field_to_fetch)

            if self.custom_function:
                continue

            projection = self.projections[i] if self.projections else None
            if projection or projection == field_to_fetch:
                continue

            writer.append(" as ")
            writer.append(projection)
