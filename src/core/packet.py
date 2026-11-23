import ctypes

FLAG_DATA = 0x01
FLAG_ACK  = 0x02
FLAG_SYN  = 0x04
FLAG_FIN  = 0x08

def compute_checksum(payload):
    if len(payload) % 2 == 1:
        payload += b'\x00'

    total = 0
    for i in range(0, len(payload), 2):
        word = (payload[i] << 8) + payload[i + 1]
        total += word
        total = (total & 0xFFFF) + (total >> 16)

    return (~total) & 0XFFFF

class PacketHeader(ctypes.BigEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('seq_num', ctypes.c_uint32),
        ('ack_num', ctypes.c_uint32),
        ('window_size', ctypes.c_uint16),
        ('flags', ctypes.c_uint8),
        ('checksum', ctypes.c_uint16),
    ]

class Packet:
    HEADER_SIZE = ctypes.sizeof(PacketHeader)

    def __init__(self, seq_num=0, ack_num=0, window_size=0, flags=0, payload=b''):
        if isinstance(payload, str):
            payload = payload.encode('utf-8')

        self.header = PacketHeader(seq_num, ack_num, window_size, flags, 0)
        self.payload = payload
        self.header.checksum = compute_checksum(bytes(self.header) + self.payload)

    def to_bytes(self):
        return bytes(self.header) + self.payload

    @classmethod
    def from_bytes(cls, raw_data):
        if len(raw_data) < cls.HEADER_SIZE:
            raise ValueError('Packet too short')

        header = PacketHeader.from_buffer_copy(raw_data[:cls.HEADER_SIZE])
        payload = raw_data[cls.HEADER_SIZE:]

        pkt = cls.__new__(cls)
        pkt.header = header
        pkt.payload = payload

        return pkt

    def is_valid(self):
        received_checksum = self.header.checksum
        self.header.checksum = 0
        calculated = compute_checksum(bytes(self.header) + self.payload)
        self.header.checksum = received_checksum
        return received_checksum == calculated