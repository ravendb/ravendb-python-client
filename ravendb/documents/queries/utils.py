import hashlib
from typing import Iterable, TYPE_CHECKING, List

from ravendb import constants
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.queries.index_query import Parameters


class HashCalculator:
    def __init__(self):
        self.__buffer = []

    @property
    def hash(self) -> str:
        return self.flush_md5()

    @staticmethod
    def __convert_to_hashable(list_):
        for item in list_:
            if isinstance(item, list):
                HashCalculator.__convert_to_hashable(item)
        return tuple(list_)

    def write(self, obj: object):
        if obj is None:
            self.__buffer.append("null-object")
        elif isinstance(obj, (bool, int, float, str, bytes, bytearray)):
            self.__buffer.append(str(obj))
        elif isinstance(obj, list):
            self.__buffer.append(str(self.__convert_to_hashable(obj)))
        elif isinstance(obj, dict):
            self.__buffer.append(str(self.__convert_to_hashable(obj.items())))
        elif "__str__" in obj.__class__.__dict__:
            self.__buffer.append(str(obj))
        else:
            self.__buffer.append(str(Utils.dictionarize(obj)))

    def write_parameters(self, qp: "Parameters") -> None:
        if qp is None:
            self.write("null-params")
        else:
            self.write(len(qp))
            for key, value in qp.items():
                self.__write_parameter_value(value)

    def __write_parameter_value(self, value: object) -> None:
        self.write(value)

    def flush_md5(self) -> str:
        return self.calculate_hash_from_str_collection(self.__buffer)

    @staticmethod
    def calculate_hash_from_str_collection(unique_ids: Iterable[str]) -> str:
        return hashlib.md5(str(hash(tuple(unique_ids))).encode("utf-8")).hexdigest()


class QueryFieldUtil:
    @staticmethod
    def __should_escape(name: str, is_path: bool) -> bool:
        escape = False
        inside_escaped = False
        first = True

        for c in name:
            if c == "'" or c == '"':
                inside_escaped = not inside_escaped
                continue

            if first:
                if not c.isalpha() and c != "_" and c != "@" and not inside_escaped:
                    escape = True
                    break
                first = False
            else:
                if (
                    (not c.isalpha() and not c.isdigit())
                    and c != "_"
                    and c != "@"
                    and c != "["
                    and c != "]"
                    and c != "("
                    and c != ")"
                    and c != "."
                    and not inside_escaped
                ):
                    escape = True
                    break

                if is_path and c == "." and not inside_escaped:
                    escape = True
                    break
        escape |= inside_escaped
        return escape

    @staticmethod
    def escape_if_necessary(name: str, is_path: bool = False) -> str:
        if (
            not name
            or name == constants.Documents.Indexing.Fields.DOCUMENT_ID_FIELD_NAME
            or name == constants.Documents.Indexing.Fields.REDUCE_KEY_HASH_FIELD_NAME
            or name == constants.Documents.Indexing.Fields.REDUCE_KEY_KEY_VALUE_FIELD_NAME
            or name == constants.Documents.Indexing.Fields.VALUE_FIELD_NAME
            or name == constants.Documents.Indexing.Fields.SPATIAL_SHAPE_FIELD_NAME
        ):
            return name

        if not QueryFieldUtil.__should_escape(name, is_path):
            return name

        sb = [name]
        need_end_quote = False
        last_term_start = 0
        offset = 0
        for i in range(len(sb)):
            if i + offset == len(sb):
                break
            c = sb[i + offset]
            if i + offset == 0 and not c.isalpha() and c != "_" and c != "@":
                sb.insert(last_term_start, "'")
                need_end_quote = True
                continue

            if is_path and c == ".":
                if need_end_quote:
                    need_end_quote = False
                    sb.insert(i + offset, "'")
                    offset += 1

                last_term_start = i + offset + 1
                continue

            if not c.isalnum() and c not in ["_", "-", "@", ".", "[", "]"] and not need_end_quote:
                sb.insert(last_term_start, "'")
                need_end_quote = True
                continue

        if need_end_quote:
            sb.append("'")

        return "".join(sb)
