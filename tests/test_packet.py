
import pytest
from src.core.packet import Packet

def test_valid_empty_payload():
    pkt = Packet(seq_num=1, ack_num=0, window_size=1024, flags=0)

    assert pkt.is_valid()
    assert pkt.payload == b''


def test_valid_packet_with_payload():
    pkt = Packet(
        seq_num=100,
        ack_num=50,
        window_size=2048,
        flags=0x01,
        payload=b'Hello World'
    )

    assert pkt.is_valid()
    assert pkt.payload == b'Hello World'

    raw_data = pkt.to_bytes()
    received_pkt = Packet.from_bytes(raw_data)

    assert received_pkt.is_valid()
    assert received_pkt.header.seq_num == 100
    assert received_pkt.header.ack_num == 50
    assert received_pkt.payload == b'Hello World'

def test_invalid_packet_too_short():
    raw_data = b'\x01\x02\x03'

    with pytest.raises(ValueError):
        Packet.from_bytes(raw_data)


def test_invalid_checksum():
    pkt = Packet(
        seq_num=10,
        ack_num=20,
        window_size=512,
        flags=0x01,
        payload=b'Test'
    )

    pkt.header.checksum ^= 0xFFFF

    assert not pkt.is_valid()


def test_invalid_payload_corruption():
    pkt = Packet(
        seq_num=10,
        ack_num=20,
        window_size=512,
        flags=0x01,
        payload=b'Hello'
    )

    pkt.payload = b'World'

    assert not pkt.is_valid()


def test_invalid_header_corruption():
    pkt = Packet(
        seq_num=10,
        ack_num=20,
        window_size=512,
        flags=0x01,
        payload=b'Test'
    )

    pkt.header.seq_num = 999

    assert not pkt.is_valid()
