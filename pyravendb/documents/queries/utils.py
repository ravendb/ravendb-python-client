import hashlib
from typing import Iterable, TYPE_CHECKING, List

from pyravendb import constants

if TYPE_CHECKING:
    from pyravendb.documents.queries.index_query import Parameters


class HashCalculator:
    def __init__(self):
        self.__buffer = []

    @property
    def hash(self) -> str:
        return self.flush_md5()

    def write_str(self, s: str):
        if s is None:
            self.write_str("null-string")
        self.__buffer.append(s)

    def write_float(self, f: float):
        if f is None:
            self.write_str("null-float")
        else:
            self.__buffer.append(str(f))

    def write_int(self, i: int):
        if i is None:
            self.write_str("null-int")
        else:
            self.__buffer.append(str(i))

    def write_bool(self, b: bool):
        if b is None:
            self.write_str("null-bool")
        else:
            self.__buffer.append(str(b))

    def write_list(self, ls: List):
        if not ls:
            self.write_str("null-list")
        for item in ls:
            self.write_object(item)

    def write_object(self, obj: object):  # todo: rewrite from Java/dicuss about implementation in Python
        if obj is None:
            self.__buffer.append("null-object")
        elif isinstance(obj, str):
            self.write_str(obj)
        elif isinstance(obj, int):
            self.write_int(obj)
        elif isinstance(obj, bool):
            self.write_bool(obj)
        elif isinstance(obj, float):
            self.write_float(obj)
        elif isinstance(obj, list):
            self.write_list(obj)
        elif "__str__" in obj.__class__.__dict__:
            self.__buffer.append(str(obj))
        else:
            raise TypeError(f"Cannot parse to string, __str__ isn't defined for {obj.__class__.__name__}")

    def write_parameters(self, qp: "Parameters") -> None:
        if qp is None:
            self.write_str("null-params")
        else:
            self.write_int(len(qp))
            for key, value in qp.items():
                self.write_str(key)
                self.__write_parameter_value(value)

    def __write_parameter_value(
        self, value: object
    ) -> None:  # todo: rewrite from Java/dicuss about implementation in Python
        self.write_object(value)

    def flush_md5(self) -> str:
        return self.calculate_hash_from_str_collection(self.__buffer)

    @staticmethod
    def calculate_hash_from_str_collection(unique_ids: Iterable[str]) -> str:
        return hashlib.md5(bytes(",".join(unique_ids).encode("utf-8"))).hexdigest()


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
