
import socket
import pytest
from unittest.mock import MagicMock, patch

from src.protocols.selective_repeat import SRSender, SRReceiver
from src.core.packet import Packet, FLAG_DATA, FLAG_ACK, FLAG_FIN


# =========================
# SENDER HELPER
# =========================

def create_sender(mock_socket=None, window_size=8):

    if mock_socket is None:
        mock_socket = MagicMock()

    with patch("src.protocols.selective_repeat.threading.Thread"):
        sender = SRSender(
            sock=mock_socket,
            dest_addr=("127.0.0.1", 5000),
            window_size=window_size,
            timeout_duration=0.5
        )

    return sender


# =========================
# SENDER TESTS
# =========================

def test_sender_successful_send():
    """Sender should create and transmit a DATA packet."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    with patch.object(sender, "_start_timer"):
        sender.send_data(b"Hello World")

    mock_socket.sendto.assert_called_once()

    raw_data = mock_socket.sendto.call_args[0][0]
    pkt = Packet.from_bytes(raw_data)

    assert pkt.is_valid()
    assert pkt.header.seq_num == 0
    assert pkt.header.flags == FLAG_DATA
    assert pkt.payload == b"Hello World"

    assert sender.next_seqnum == 1
    assert 0 in sender.buffer


def test_sender_effective_window():
    """Effective window should respect sender and receiver limits."""

    sender = create_sender(window_size=8)

    sender.rwnd = 4
    assert sender._effective_window() == 4

    sender.rwnd = 12
    assert sender._effective_window() == 8

    sender.rwnd = 0
    assert sender._effective_window() == 1


def test_sender_ack_received():
    """Sender should remove only the individually acknowledged packet."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    pkt0 = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")
    pkt1 = Packet(seq_num=1, flags=FLAG_DATA, payload=b"Second")

    sender.buffer = {0: pkt0, 1: pkt1}
    sender.next_seqnum = 2

    ack_pkt = Packet(
        seq_num=0,
        ack_num=0,
        window_size=5,
        flags=FLAG_ACK
    )

    def receive_ack(*args):
        sender.running = False
        return ack_pkt.to_bytes(), ("127.0.0.1", 5000)

    mock_socket.recvfrom.side_effect = receive_ack

    with patch.object(sender, "_stop_timer") as stop_timer:
        sender._listen_for_acks()

    assert 0 not in sender.buffer
    assert 1 in sender.buffer

    assert sender.base == 1
    assert sender.rwnd == 5

    stop_timer.assert_called_once_with(0)


def test_sender_timeout_retransmission():
    """Sender should retransmit only the packet whose timer expires."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    pkt0 = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")
    pkt1 = Packet(seq_num=1, flags=FLAG_DATA, payload=b"Second")

    sender.buffer = {0: pkt0, 1: pkt1}

    with patch.object(sender, "_start_timer") as start_timer:
        sender._on_timeout(0)

    mock_socket.sendto.assert_called_once_with(
        pkt0.to_bytes(),
        sender.dest_addr
    )

    start_timer.assert_called_once_with(0)


def test_sender_timeout_acknowledged_packet():
    """Sender should not retransmit a packet that has already been ACKed."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    pkt = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")

    # Packet is no longer in the buffer.
    sender.buffer = {}

    sender._on_timeout(0)

    mock_socket.sendto.assert_not_called()

    sender.close()


def test_sender_close():
    """Sender should transmit FIN and stop running when closing."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    sender.close()

    mock_socket.sendto.assert_called_once()

    raw_data = mock_socket.sendto.call_args[0][0]
    fin_pkt = Packet.from_bytes(raw_data)

    assert fin_pkt.is_valid()
    assert fin_pkt.header.flags == FLAG_FIN
    assert sender.running is False


# =========================
# RECEIVER HELPER
# =========================

def create_receiver(mock_socket, output_path, max_buffer_packets=16):

    return SRReceiver(
        sock=mock_socket,
        output_path=str(output_path),
        max_buffer_packets=max_buffer_packets
    )


# =========================
# RECEIVER TESTS
# =========================

def test_receiver_in_order_packet(tmp_path):
    """Receiver should deliver an in-order packet and send an ACK."""

    mock_socket = MagicMock()

    pkt = Packet(
        seq_num=0,
        flags=FLAG_DATA,
        payload=b"Hello World"
    )

    fin_pkt = Packet(seq_num=1, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    assert output_file.read_bytes() == b"Hello World"
    assert receiver.rcv_base == 1

    mock_socket.sendto.assert_called_once()

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args[0][0]
    )

    assert ack_pkt.is_valid()
    assert ack_pkt.header.flags == FLAG_ACK
    assert ack_pkt.header.ack_num == 0


def test_receiver_out_of_order_packet_buffered(tmp_path):
    """Receiver should buffer an out-of-order packet without delivering it."""

    mock_socket = MagicMock()

    pkt = Packet(
        seq_num=1,
        flags=FLAG_DATA,
        payload=b"Second"
    )

    fin_pkt = Packet(seq_num=2, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    # Packet 1 is buffered because packet 0 is missing.
    assert receiver.rcv_base == 0
    assert receiver.buffer == {1: b"Second"}

    # Nothing should have been delivered yet.
    assert output_file.read_bytes() == b""

    # Receiver still ACKs the buffered packet.
    assert mock_socket.sendto.call_count == 1

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args[0][0]
    )

    assert ack_pkt.header.ack_num == 1
    assert ack_pkt.header.flags == FLAG_ACK


def test_receiver_delivers_buffered_packets_in_order(tmp_path):
    """Receiver should deliver buffered packets once the missing packet arrives."""

    mock_socket = MagicMock()

    pkt1 = Packet(seq_num=1, flags=FLAG_DATA, payload=b"Second")
    pkt0 = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")
    fin_pkt = Packet(seq_num=2, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (pkt1.to_bytes(), ("127.0.0.1", 5000)),
        (pkt0.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    # Even though packet 1 arrived first, delivery must be ordered.
    assert output_file.read_bytes() == b"FirstSecond"

    assert receiver.rcv_base == 2
    assert receiver.buffer == {}

    assert mock_socket.sendto.call_count == 2


def test_receiver_duplicate_packet(tmp_path):
    """Receiver should ACK duplicate packets without delivering them again."""

    mock_socket = MagicMock()

    pkt0 = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")
    duplicate_pkt = Packet(
        seq_num=0,
        flags=FLAG_DATA,
        payload=b"First"
    )

    fin_pkt = Packet(seq_num=1, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (pkt0.to_bytes(), ("127.0.0.1", 5000)),
        (duplicate_pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    # Duplicate must not be written twice.
    assert output_file.read_bytes() == b"First"

    assert receiver.rcv_base == 1

    # ACK original packet and duplicate packet.
    assert mock_socket.sendto.call_count == 2

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args_list[1].args[0]
    )

    assert ack_pkt.is_valid()
    assert ack_pkt.header.ack_num == 0
    assert ack_pkt.header.flags == FLAG_ACK


def test_receiver_corrupted_packet(tmp_path):
    """Receiver should drop corrupted packets without sending an ACK."""

    mock_socket = MagicMock()

    pkt = Packet(
        seq_num=0,
        flags=FLAG_DATA,
        payload=b"Hello"
    )

    raw_data = bytearray(pkt.to_bytes())
    raw_data[-1] ^= 0x01

    fin_pkt = Packet(seq_num=0, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (bytes(raw_data), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    assert receiver.rcv_base == 0
    assert output_file.read_bytes() == b""

    mock_socket.sendto.assert_not_called()


def test_receiver_malformed_packet(tmp_path):
    """Receiver should ignore packets smaller than the header."""

    mock_socket = MagicMock()

    fin_pkt = Packet(seq_num=0, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (b"\x01\x02\x03", ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    assert receiver.rcv_base == 0
    assert receiver.buffer == {}
    assert output_file.read_bytes() == b""

    mock_socket.sendto.assert_not_called()


def test_receiver_packet_outside_window(tmp_path):
    """Receiver should ignore packets outside its receiving window."""

    mock_socket = MagicMock()

    # Window size is 2, so valid sequence numbers are 0 and 1.
    pkt = Packet(
        seq_num=2,
        flags=FLAG_DATA,
        payload=b"Outside"
    )

    fin_pkt = Packet(seq_num=0, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(
        mock_socket,
        output_file,
        max_buffer_packets=2
    )

    receiver.start()

    assert receiver.rcv_base == 0
    assert receiver.buffer == {}
    assert output_file.read_bytes() == b""

    # Packet outside the window is ignored without an ACK.
    mock_socket.sendto.assert_not_called()


def test_receiver_fin_packet(tmp_path):
    """Receiver should terminate when it receives a valid FIN packet."""

    mock_socket = MagicMock()

    fin_pkt = Packet(
        seq_num=0,
        flags=FLAG_FIN
    )

    mock_socket.recvfrom.return_value = (
        fin_pkt.to_bytes(),
        ("127.0.0.1", 5000)
    )

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    assert receiver.rcv_base == 0
    assert receiver.buffer == {}

    mock_socket.sendto.assert_not_called()
