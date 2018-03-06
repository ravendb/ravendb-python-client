import ijson
import re

LEXEME_RE = re.compile(r'[a-z0-9eE\.\+-]+|\S')


# The code imported from ijson to be able to receive json from socket
class IncrementalJsonParser:
    def __init__(self, socket):
        self.lexer = self.lexer(socket)

    @staticmethod
    def basic_parse(response, buf_size=ijson.backend.BUFSIZE):
        """
        Iterator yielding unprefixed events.

        Parameters:

        - response: a stream response from requests
        """
        lexer = iter(IncrementalJsonParser.lexer(response, buf_size))
        for value in ijson.backend.parse_value(lexer):
            yield value
        try:
            next(lexer)
        except StopIteration:
            pass
        else:
            raise ijson.common.JSONError('Additional data')

    @staticmethod
    def lexer(response, buf_size=ijson.backend.BUFSIZE):
        it = response.iter_content(buf_size)
        data = next(it, None)
        if data is None:
            yield None
        buf = data.decode('utf-8')
        pos = 0
        discarded = 0
        while True:
            match = LEXEME_RE.search(buf, pos)
            if pos < len(buf) and match:
                lexeme = match.group()
                if lexeme == '"':
                    pos = match.start()
                    start = pos + 1
                    while True:
                        try:
                            end = buf.index('"', start)
                            escpos = end - 1
                            while buf[escpos] == '\\':
                                escpos -= 1
                            if (end - escpos) % 2 == 0:
                                start = end + 1
                            else:
                                break
                        except ValueError:
                            data = next(it, buf_size)
                            if not data:
                                yield None
                            buf += data.decode('utf-8')
                    yield discarded + pos, buf[pos:end + 1]
                    pos = end + 1
                else:
                    while match.end() == len(buf):
                        data = next(it, None)
                        if not data:
                            break
                        buf += data.decode('utf-8')
                        match = LEXEME_RE.search(buf, pos)
                        lexeme = match.group()
                    pos = match.end()
                    yield discarded + match.start(), lexeme
            else:
                data = next(it, None)
                if data is None:
                    yield None
                buf = data.decode('utf-8')
                pos = 0
