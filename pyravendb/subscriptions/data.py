from pyravendb.tools.utils import Utils
from enum import Enum
from datetime import timedelta
import ijson
from _elementtree import ParseError
from socket import timeout
import re


class SubscriptionOpeningStrategy(Enum):
    """
    The client will successfully open a subscription only if there isn't any other currently connected client.
    Otherwise it will end up with SubscriptionInUseException.
    """
    open_if_free = "OpenIfFree"
    """
    The connecting client will successfully open a subscription even if there is another active subscription's consumer.
    If the new client takes over an existing client then the existing one will get a SubscriptionInUseException.

    The subscription will always be held by the last connected client.
    """
    take_over = "TakeOver"
    """
    If the client currently cannot open the subscription because it is used by another client. it will wait for that 
    client to complete and keep attempting to gain the subscription
    """
    wait_for_free = "WaitForFree"

    def __str__(self):
        return self.value


class SubscriptionCreationOptions:
    def __init__(self, query, name=None, change_vector=None):
        if not query:
            raise ValueError("Invalid subscriptions query")
        self.name = name
        self.change_vector = change_vector
        self.query = query

    def to_json(self):
        return {"ChangeVector": self.change_vector, "Query": self.query, "Name": self.name}


class SubscriptionWorkerOptions:
    def __init__(self, subscription_name, strategy=None, ignore_subscriber_errors=False,
                 time_to_wait_before_connection_retry=None, max_docs_per_batch=4096, max_erroneous_period=None,
                 close_when_no_docs_left=False):
        """
        @param str subscription_name: The subscription name
        @param SubscriptionOpeningStrategy strategy: Options for opening a subscription
        @param ignore_subscriber_errors: Ignore subscriber errors
        @param time_to_wait_before_connection_retry: The time to wait for retry
        @param int max_docs_per_batch: The max docs to send in one batch
        @param bool close_when_no_docs_left: Will continue the subscription work until the server have no more new documents to send
        """
        if not subscription_name:
            raise AttributeError("Value cannot be null or empty.", "subscription_name")
        self.subscription_name = subscription_name
        self.strategy = strategy if strategy is not None else SubscriptionOpeningStrategy.open_if_free
        self.ignore_subscriber_errors = ignore_subscriber_errors
        if time_to_wait_before_connection_retry is None:
            time_to_wait_before_connection_retry = timedelta(milliseconds=5000)
        self.time_to_wait_before_connection_retry = time_to_wait_before_connection_retry
        self.max_docs_per_batch = max_docs_per_batch
        self.max_erroneous_period = max_erroneous_period
        if max_erroneous_period is None:
            self.max_erroneous_period = timedelta(minutes=5)
        self.close_when_no_docs_left = close_when_no_docs_left

    def to_json(self):
        return {"SubscriptionName": self.subscription_name,
                "TimeToWaitBeforeConnectionRetry": str(self.time_to_wait_before_connection_retry),
                "Strategy": str(self.strategy), "MaxDocsPerBatch": self.max_docs_per_batch,
                "IgnoreSubscriberErrors": self.ignore_subscriber_errors,
                "CloseWhenNoDocsLeft": self.close_when_no_docs_left}


class SubscriptionState:
    def __init__(self, query=None, change_vector_for_next_batch_starting_point=None, subscription_id=0,
                 subscription_name=None, last_time_server_made_progress_with_documents=None,
                 last_client_connection_time=None, disabled=False, **kwargs):
        self.query = query
        self.change_vector_for_next_batch_starting_point = change_vector_for_next_batch_starting_point
        self.subscription_id = subscription_id
        self.subscription_name = subscription_name
        if isinstance(last_time_server_made_progress_with_documents, str):
            last_time_server_made_progress_with_documents = Utils.string_to_datetime(
                last_time_server_made_progress_with_documents)
        self.last_time_server_made_progress_with_documents = last_time_server_made_progress_with_documents
        if isinstance(last_client_connection_time, str):
            last_client_connection_time = Utils.string_to_datetime(last_client_connection_time)
        self.last_client_connection_time = last_client_connection_time
        self.closed = disabled
        self.latest_change_vector_client_acknowledged = kwargs.get("LatestChangeVectorClientACKnowledged", None)

    def to_json(self):
        return {"Query": self.query,
                "ChangeVectorForNextBatchStartingPoint": self.change_vector_for_next_batch_starting_point,
                "SubscriptionId": self.subscription_id, "SubscriptionName": self.subscription_name,
                "LastTimeServerMadeProgressWithDocuments": Utils.datetime_to_string(
                    self.last_time_server_made_progress_with_documents),
                "LastClientConnectionTime": Utils.datetime_to_string(self.last_client_connection_time),
                "Disabled": self.closed}


LEXEME_RE = re.compile(b'[a-z0-9eE\.\+-]+|\S')
BYTE_ARRAY_CHARACTERS = bytearray(b',}:{"')
IS_WEBSOCKET = False


# The code imported from ijson to be able to receive json from socket
class IncrementalJsonParser:
    def __init__(self, socket, is_websocket=False):
        global IS_WEBSOCKET
        IS_WEBSOCKET = is_websocket
        self.lexer = self.lexer(socket)

    @staticmethod
    def lexer(socket, buf_size=ijson.backend.BUFSIZE):
        data = socket.recv() if IS_WEBSOCKET else socket.recv(buf_size)
        if not data:
            return

        buf = data[1:-1].encode('utf-8') if IS_WEBSOCKET else data
        pos = 0
        discarded = 0
        while True:
            match = LEXEME_RE.search(buf, pos)
            if pos < len(buf) and match:
                lexeme = match.group()
                if lexeme == b'"':
                    pos = match.start()
                    start = pos + 1
                    while True:
                        try:
                            end = buf.index(b'"', start)
                            escpos = end - 1
                            while buf[escpos] == '\\':
                                escpos -= 1
                            if (end - escpos) % 2 == 0:
                                start = end + 1
                            else:
                                break
                        except ValueError:
                            data = socket.recv().encode('utf-8') if IS_WEBSOCKET else socket.recv(buf_size)
                            if not data:
                                raise ijson.backend.common.IncompleteJSONError('Incomplete string lexeme')

                            buf += data[1:-1] if IS_WEBSOCKET else data

                    yield discarded + pos, buf[pos:end + 1].decode('utf-8')
                    pos = end + 1
                else:
                    while match.end() == len(buf) and buf[pos] not in BYTE_ARRAY_CHARACTERS:
                        try:
                            data = socket.recv().encode('utf-8') if IS_WEBSOCKET else socket.recv(buf_size)
                            if not data:
                                break
                            buf += data[1:-1].encode('utf-8') if IS_WEBSOCKET else data
                            match = LEXEME_RE.search(buf, pos)
                            lexeme = match.group()
                        except timeout:
                            break

                    yield match.start(), lexeme.decode('utf-8')
                    pos = match.end()
            else:
                data = socket.recv().encode('utf-8') if IS_WEBSOCKET else socket.recv(buf_size)
                if not data:
                    break
                discarded += len(buf)
                buf = data[1:-1] if IS_WEBSOCKET else data
                pos = 0

    def create_array(self, gen):
        arr = []

        for (token, val) in gen:
            if token == "end_array":
                return arr
            arr.append(self.get_value_from_token(gen, token, val))

        raise ParseError("End array expected, but the generator ended before we got it")

    def get_value(self, gen):
        (token, val) = next(gen)

        return self.get_value_from_token(gen, token, val)

    def get_value_from_token(self, gen, token, val):
        if token == "start_array":
            return self.create_array(gen)

        if token == "start_map":
            return self.create_object(gen)

        return val

    def create_object(self, gen):
        obj = {}

        for (token, val) in gen:
            if token == "end_map":
                return obj
            if token == "map_key":
                obj[val] = self.get_value(gen)

        raise ParseError("End object expected, but the generator ended before we got it")

    def next_object(self):

        try:
            (_, text) = next(self.lexer)
            if IS_WEBSOCKET and text == ',':
                (_, text) = next(self.lexer)
        except StopIteration:
            return None

        if text != '{':
            raise ParseError("Expected start object, got: " + text)

        gen = IncrementalJsonParser.parse_object(self.lexer)
        (token, val) = next(gen)
        assert token == "start_map"

        return self.create_object(gen)

    @staticmethod
    def parse_value(lexer, symbol=None, pos=0):
        try:
            if symbol is None:
                pos, symbol = next(lexer)
            if symbol == 'null':
                yield ('null', None)
            elif symbol == 'true':
                yield ('boolean', True)
            elif symbol == 'false':
                yield ('boolean', False)
            elif symbol == '[':
                for event in IncrementalJsonParser.parse_array(lexer):
                    yield event
            elif symbol == '{':
                for event in IncrementalJsonParser.parse_object(lexer):
                    yield event
            elif symbol[0] == '"':
                yield ('string', IncrementalJsonParser.unescape(symbol[1:-1]))
            else:
                # if we got a partial token for false / null / true we need to read from the network again
                while symbol[0] in ('t', 'n') and len(symbol) < 4 or symbol[0] == 'f' and len(symbol) < 5:
                    _, nextpart = next(lexer)
                    if symbol == 'null':
                        yield ('null', None)
                    elif symbol == 'true':
                        yield ('boolean', True)
                    elif symbol == 'false':
                        yield ('boolean', False)
                    return

                try:
                    yield ('number', ijson.backend.common.number(symbol))
                except ijson.backend.decimal.InvalidOperation:
                    raise ijson.backend.UnexpectedSymbol(symbol, pos)
        except StopIteration:
            raise ijson.backend.common.IncompleteJSONError('Incomplete JSON data')

    @staticmethod
    def parse_array(lexer):
        yield ('start_array', None)
        try:
            pos, symbol = next(lexer)
            if symbol != ']':
                while True:
                    for event in IncrementalJsonParser.parse_value(lexer, symbol, pos):
                        yield event
                    pos, symbol = next(lexer)
                    if symbol == ']':
                        break
                    if symbol != ',':
                        raise ijson.backend.UnexpectedSymbol(symbol, pos)
                    pos, symbol = next(lexer)
            yield ('end_array', None)
        except StopIteration:
            raise ijson.backend.common.IncompleteJSONError('Incomplete JSON data')

    @staticmethod
    def parse_object(lexer):
        yield ('start_map', None)
        try:
            pos, symbol = next(lexer)
            if symbol != '}':
                while True:
                    if symbol[0] != '"':
                        raise ijson.backend.UnexpectedSymbol(symbol, pos)
                    yield ('map_key', IncrementalJsonParser.unescape(symbol[1:-1]))
                    pos, symbol = next(lexer)
                    if symbol != ':':
                        raise ijson.backend.UnexpectedSymbol(symbol, pos)
                    for event in IncrementalJsonParser.parse_value(lexer, None, pos):
                        yield event
                    pos, symbol = next(lexer)
                    if symbol == '}':
                        break
                    if symbol != ',':
                        raise ijson.backend.UnexpectedSymbol(symbol, pos)
                    pos, symbol = next(lexer)
            yield ('end_map', None)
        except StopIteration:
            raise ijson.backend.common.IncompleteJSONError('Incomplete JSON data')

    @staticmethod
    def unescape(s):
        start = 0
        result = ''
        while start < len(s):
            pos = s.find('\\', start)
            if pos == -1:
                if start == 0:
                    return s
                result += s[start:]
                break
            result += s[start:pos]
            pos += 1
            esc = s[pos]
            if esc == 'u':
                result += chr(int(s[pos + 1:pos + 5], 16))
                pos += 4
            elif esc == 'b':
                result += '\b'
            elif esc == 'f':
                result += '\f'
            elif esc == 'n':
                result += '\n'
            elif esc == 'r':
                result += '\r'
            elif esc == 't':
                result += '\t'
            else:
                result += esc
            start = pos + 1
        return result
