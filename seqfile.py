import StringIO
import struct
import typedbytes

def typedbytes_reader(buf):
    str = StringIO.StringIO(buf)
    tb_in = typedbytes.Input(str)
    return tb_in.read()

reader_registry = {
    'org.apache.hadoop.typedbytes.TypedBytesWritable': typedbytes_reader
}


class Stream(object):

    def __init__(self, source):
        self._src = source

    def read_bytes(self, num):
        data = self._src.read(num)
        if len(data) < num:
            raise EOFError
        return data

    def read_int(self):
        data = self.read_bytes(4)
        return struct.unpack('!i', data)[0]

    def read_byte(self):
        byte = self.read_bytes(1)
        return struct.unpack('!b', byte)[0]

    def read_string(self):
        str_len = self.read_vint()
        return unicode(self.read_bytes(str_len), 'utf-8')

    def read_bool(self):
        return bool(self.read_byte())

    def read_vint(self):
        first = self.read_byte()
        l = self._decode_vint_size(first)
        if l == 1:
            return first
        x = 0
        for i in range(l-1):
            b = self.read_byte()
            x = x << 8
            x = x | (b & 0xFF)
        if self._is_negative_vint(first):
            return  x ^ -1
        return x

    def _is_negative_vint(self, val):
        return val < -120 or (val >= -122 and val < 0)

    def _decode_vint_size(self, val):
        if val >= -122:
            return 1
        elif val < -120:
            return -119 -val
        return -111 -val

    def tell(self):
        return self._src.tell()

    def seek(self, pos):
        self._src.seek(pos)


class SequenceFileReader(object):

    def __init__(self, seqfile):
        self.stream = Stream(seqfile)
        self.version = None
        self.key_class = None
        self.compression_class = None
        self.value_class = None
        self.compression = False
        self.block_compression = False
        self.meta = {}
        self.sync = None
        self._read_header()
        if self.compression or self.block_compression:
            raise NotImplementedError("reading of seqfiles with compression is not implemented.")
        try:
            self.key_reader = reader_registry[self.key_class]
        except KeyError:
            raise ValueError("no known reader for key type '%s'" % self.key_class)
        try:
            self.value_reader = reader_registry[self.value_class]
        except KeyError:
            raise ValueError("no known reader for value type '%s'" % self.value_class)

    def _read_header(self):
        stream = self.stream
        seq = stream.read_bytes(3)
        if seq != "SEQ":
            raise ValueError("given file is not a sequence-file")
        self.version = stream.read_byte()
        self.key_class = stream.read_string()
        self.value_class = stream.read_string()
        self.compression = stream.read_bool()
        self.block_compression = stream.read_bool()
        if self.compression:
            self.compression_class = stream.read_string()
        meta_len = stream.read_int()
        for i in range(meta_len):
            key = stream.read_string()
            val = stream.read_string()
            self.meta[key] = val
        self.sync = stream.read_bytes(16)

    def __iter__(self):
        while True:
            try:
                next = self.load()
            except EOFError:
                raise StopIteration
            yield next

    def load(self):
        stream = self.stream
        buf_len = stream.read_int()
        if buf_len == -1:
            syncCheck = stream.read_bytes(16)
            if syncCheck != self.sync:
                raise ValueError("file corrupt")
            buf_len = stream.read_int()
        key_len = stream.read_int()
        buf = stream.read_bytes(buf_len)
        ikey_len, key_buf = buffer(buf, 0, 4), buffer(buf, 4, key_len-4)
        ivalue_len, value_buf = buffer(buf, key_len, 4), buffer(buf, key_len+4)
        key = self.key_reader(key_buf)
        value = self.value_reader(value_buf)
        return key, value

    def tell(self):
        return self.stream.tell()

    def seek(self, pos):
        self.stream.seek(pos)



class ValueOnlySequenceFileReader(SequenceFileReader):

    def load(self):
        k, v = SequenceFileReader.load(self)
        return v
