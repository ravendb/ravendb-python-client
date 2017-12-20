import ijson
import re

LEXEME_RE = re.compile(r'[a-z0-9eE\.\+-]+|\S')


# The code imported from ijson to be able to receive json from socket
class IncrementalJsonParser:
    def __init__(self, socket):
        self.lexer = self.lexer(socket)

    @staticmethod
    def basic_parse(response, buf_size=ijson.backend.BUFSIZE):
        '''
        Iterator yielding unprefixed events.

        Parameters:

        - response: a stream response from requests
        '''
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
                        data = next(data, None)
                        if not data:
                            break
                        buf += data
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

    # def create_array(self, gen):
    #     arr = []
    #
    #     for (token, val) in gen:
    #         if token == "end_array":
    #             return arr
    #         arr.append(self.get_value(gen))
    #
    #     raise ParseError("End array expected, but the generator ended before we got it")
    #
    # def get_value(self, gen):
    #     (token, val) = next(gen)
    #
    #     if token == "start_array":
    #         return self.create_array(gen)
    #
    #     if token == "start_map":
    #         return self.create_object(gen)
    #
    #     return val
    #
    # def create_object(self, gen):
    #     obj = {}
    #
    #     for (token, val) in gen:
    #         if token == "end_map":
    #             return obj
    #         if token == "map_key":
    #             obj[val] = self.get_value(gen)
    #
    #     raise ParseError("End object expected, but the generator ended before we got it")
    #
    # def next_object(self):
    #
    #     try:
    #         (_, text) = next(self.lexer)
    #     except StopIteration:
    #         return None
    #
    #     if text != '{':
    #         raise ParseError("Expected start object, got: " + text)
    #
    #     gen = IncrementalJsonParser.parse_object(self.lexer)
    #     (token, val) = next(gen)
    #     assert token == "start_map"
    #
    #     return self.create_object(gen)
    #
    # @staticmethod
    # def parse_value(lexer, symbol=None, pos=0):
    #     try:
    #         if symbol is None:
    #             pos, symbol = next(lexer)
    #         if symbol == 'null':
    #             yield ('null', None)
    #         elif symbol == 'true':
    #             yield ('boolean', True)
    #         elif symbol == 'false':
    #             yield ('boolean', False)
    #         elif symbol == '[':
    #             for event in IncrementalJsonParser.parse_array(lexer):
    #                 yield event
    #         elif symbol == '{':
    #             for event in IncrementalJsonParser.parse_object(lexer):
    #                 yield event
    #         elif symbol[0] == '"':
    #             yield ('string', IncrementalJsonParser.unescape(symbol[1:-1]))
    #         else:
    #             # if we got a partial token for false / null / true we need to read from the network again
    #             while symbol[0] in ('t', 'n') and len(symbol) < 4 or symbol[0] == 'f' and len(symbol) < 5:
    #                 _, nextpart = next(lexer)
    #                 if symbol == 'null':
    #                     yield ('null', None)
    #                 elif symbol == 'true':
    #                     yield ('boolean', True)
    #                 elif symbol == 'false':
    #                     yield ('boolean', False)
    #                 return
    #
    #             try:
    #                 yield ('number', symbol)
    #             except Exception as e:
    #                 print(e)
    #                 raise ijson.basic_parse.UnexpectedSymbol(symbol, pos)
    #     except StopIteration:
    #         raise ijson.basic_parse.common.IncompleteJSONError('Incomplete JSON data')
    #
    # @staticmethod
    # def parse_array(lexer):
    #     yield ('start_array', None)
    #     try:
    #         pos, symbol = next(lexer)
    #         if symbol != ']':
    #             while True:
    #                 for event in IncrementalJsonParser.parse_value(lexer, symbol, pos):
    #                     yield event
    #                 pos, symbol = next(lexer)
    #                 if symbol == ']':
    #                     break
    #                 if symbol != ',':
    #                     raise ijson.basic_parse.UnexpectedSymbol(symbol, pos)
    #                 pos, symbol = next(lexer)
    #         yield ('end_array', None)
    #     except StopIteration:
    #         raise ijson.basic_parse.common.IncompleteJSONError('Incomplete JSON data')
    #
    # @staticmethod
    # def parse_object(lexer):
    #     yield ('start_map', None)
    #     try:
    #         pos, symbol = next(lexer)
    #         if symbol != '}':
    #             while True:
    #                 if symbol[0] != '"':
    #                     raise ijson.basic_parse.UnexpectedSymbol(symbol, pos)
    #                 yield ('map_key', IncrementalJsonParser.unescape(symbol[1:-1]))
    #                 pos, symbol = next(lexer)
    #                 if symbol != ':':
    #                     raise ijson.basic_parse.UnexpectedSymbol(symbol, pos)
    #                 for event in IncrementalJsonParser.parse_value(lexer, None, pos):
    #                     yield event
    #                 pos, symbol = next(lexer)
    #                 if symbol == '}':
    #                     break
    #                 if symbol != ',':
    #                     raise ijson.basic_parse.UnexpectedSymbol(symbol, pos)
    #                 pos, symbol = next(lexer)
    #         yield ('end_map', None)
    #     except StopIteration:
    #         raise ijson.basic_parse.common.IncompleteJSONError('Incomplete JSON data')
    #
    # @staticmethod
    # def unescape(s):
    #     start = 0
    #     result = ''
    #     while start < len(s):
    #         pos = s.find('\\', start)
    #         if pos == -1:
    #             if start == 0:
    #                 return s
    #             result += s[start:]
    #             break
    #         result += s[start:pos]
    #         pos += 1
    #         esc = s[pos]
    #         if esc == 'u':
    #             result += chr(int(s[pos + 1:pos + 5], 16))
    #             pos += 4
    #         elif esc == 'b':
    #             result += '\b'
    #         elif esc == 'f':
    #             result += '\f'
    #         elif esc == 'n':
    #             result += '\n'
    #         elif esc == 'r':
    #             result += '\r'
    #         elif esc == 't':
    #             result += '\t'
    #         else:
    #             result += esc
    #         start = pos + 1
    #     return result
