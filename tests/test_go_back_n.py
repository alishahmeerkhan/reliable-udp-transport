import socket
import pytest
from unittest.mock import MagicMock, patch

from src.protocols.go_back_n import GBNSender, GBNReceiver
from src.core.packet import Packet, FLAG_DATA, FLAG_ACK, FLAG_FIN


# =========================
# SENDER TESTS
# =========================

def create_sender(sock=None, window_size=8):
    """Create a sender without starting a real ACK thread."""

    if sock is None:
        sock = MagicMock()

    with patch("src.protocols.go_back_n.threading.Thread"):
        sender = GBNSender(
            sock=sock,
            dest_addr=("127.0.0.1", 5000),
            window_size=window_size,
            timeout_duration=0.5
        )

    return sender


def test_sender_successful_send():
    """Sender should construct and transmit a DATA packet."""

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
    assert len(sender.buffer) == 1


def test_sender_effective_window():
    """Effective window should respect both cwnd and the receiver's advertised window."""

    sender = create_sender(window_size=8)


    sender.rwnd = 8
    assert sender._effective_window() == 1

    sender.congestion.cwnd = 8.0
    sender.rwnd = 3
    assert sender._effective_window() == 3

    sender.rwnd = 0
    assert sender._effective_window() == 1

    sender.close()


def test_sender_ack_received():
    """Sender should remove acknowledged packets from its buffer."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)
    sender.congestion.cwnd = 8.0

    with patch.object(sender, "_start_timer"):
        sender.send_data(b"Packet 0")
        sender.send_data(b"Packet 1")

    assert len(sender.buffer) == 2

    ack_pkt = Packet(
        seq_num=0,
        ack_num=0,
        window_size=5,
        flags=FLAG_ACK
    )

    # Stop the ACK listener after delivering one ACK.
    def receive_ack(*args):
        sender.running = False
        return ack_pkt.to_bytes(), ("127.0.0.1", 5000)

    mock_socket.recvfrom.side_effect = receive_ack

    with patch.object(sender, "_stop_timer"):
        with patch.object(sender, "_start_timer"):
            sender._listen_for_acks()

    assert sender.base == 1
    assert sender.rwnd == 5
    assert len(sender.buffer) == 1
    assert sender.buffer[0].header.seq_num == 1



def test_sender_timeout_retransmission():
    """Sender should retransmit all unacknowledged packets after timeout."""

    mock_socket = MagicMock()
    sender = create_sender(mock_socket)

    pkt1 = Packet(seq_num=0, flags=FLAG_DATA, payload=b"First")
    pkt2 = Packet(seq_num=1, flags=FLAG_DATA, payload=b"Second")

    sender.buffer = [pkt1, pkt2]

    with patch.object(sender, "_start_timer"):
        sender._on_timeout()

    assert mock_socket.sendto.call_count == 2

    sent_packets = [
        Packet.from_bytes(call.args[0])
        for call in mock_socket.sendto.call_args_list
    ]

    assert sent_packets[0].header.seq_num == 0
    assert sent_packets[1].header.seq_num == 1



def test_sender_close():
    """Sender should transmit a FIN packet when closing."""

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
# RECEIVER TESTS
# =========================

def create_receiver(mock_socket, output_path):
    """Create a receiver."""

    return GBNReceiver(
        sock=mock_socket,
        output_path=str(output_path),
        max_buffer_packets=16
    )


def test_receiver_in_order_packet(tmp_path):
    """Receiver should deliver an expected packet and send an ACK."""

    mock_socket = MagicMock()

    data_pkt = Packet(
        seq_num=0,
        flags=FLAG_DATA,
        payload=b"Hello World"
    )

    fin_pkt = Packet(
        seq_num=1,
        flags=FLAG_FIN
    )

    mock_socket.recvfrom.side_effect = [
        (data_pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    output_file = tmp_path / "received.txt"

    receiver = create_receiver(mock_socket, output_file)
    receiver.start()

    assert output_file.read_bytes() == b"Hello World"
    assert receiver.expected_seqnum == 1

    # One ACK should have been sent.
    assert mock_socket.sendto.call_count == 1

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args[0][0]
    )

    assert ack_pkt.is_valid()
    assert ack_pkt.header.flags == FLAG_ACK
    assert ack_pkt.header.ack_num == 0


def test_receiver_out_of_order_packet(tmp_path):
    """Receiver should reject out-of-order packets and send a duplicate ACK."""

    mock_socket = MagicMock()

    output_file = tmp_path / "received.txt"
    receiver = create_receiver(mock_socket, output_file)

    # Simulate that packet 0 was already delivered.
    receiver.expected_seqnum = 1

    out_of_order_pkt = Packet(
        seq_num=2,
        flags=FLAG_DATA,
        payload=b"Out of order"
    )

    fin_pkt = Packet(seq_num=3, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (out_of_order_pkt.to_bytes(), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    receiver.start()

    # Out-of-order packet must not be written.
    assert output_file.read_bytes() == b""

    assert receiver.expected_seqnum == 1

    # Duplicate ACK for the last correctly received packet.
    mock_socket.sendto.assert_called_once()

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args[0][0]
    )

    assert ack_pkt.is_valid()
    assert ack_pkt.header.flags == FLAG_ACK
    assert ack_pkt.header.ack_num == 0


def test_receiver_corrupted_packet(tmp_path):
    """Receiver should reject corrupted packets and send a duplicate ACK."""

    mock_socket = MagicMock()

    output_file = tmp_path / "received.txt"
    receiver = create_receiver(mock_socket, output_file)

    # Simulate that packet 0 was already delivered.
    receiver.expected_seqnum = 1

    pkt = Packet(
        seq_num=1,
        flags=FLAG_DATA,
        payload=b"Hello"
    )

    # Corrupt one byte of the serialized packet.
    raw_data = bytearray(pkt.to_bytes())
    raw_data[-1] ^= 0x01

    fin_pkt = Packet(seq_num=2, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (bytes(raw_data), ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    receiver.start()

    assert output_file.read_bytes() == b""
    assert receiver.expected_seqnum == 1

    mock_socket.sendto.assert_called_once()

    ack_pkt = Packet.from_bytes(
        mock_socket.sendto.call_args[0][0]
    )

    assert ack_pkt.is_valid()
    assert ack_pkt.header.ack_num == 0
    assert ack_pkt.header.flags == FLAG_ACK


def test_receiver_malformed_packet(tmp_path):
    """Receiver should ignore packets that are shorter than the header."""

    mock_socket = MagicMock()

    output_file = tmp_path / "received.txt"
    receiver = create_receiver(mock_socket, output_file)

    fin_pkt = Packet(seq_num=0, flags=FLAG_FIN)

    mock_socket.recvfrom.side_effect = [
        (b"\x01\x02\x03", ("127.0.0.1", 5000)),
        (fin_pkt.to_bytes(), ("127.0.0.1", 5000))
    ]

    receiver.start()

    assert receiver.expected_seqnum == 0
    assert output_file.read_bytes() == b""

    # Malformed packet should simply be ignored.
    mock_socket.sendto.assert_not_called()


def test_receiver_fin_packet(tmp_path):
    """Receiver should terminate when a valid FIN packet is received."""

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

    assert receiver.expected_seqnum == 0
    assert output_file.read_bytes() == b""

    # Receiver does not send an ACK for FIN in the current implementation.
    mock_socket.sendto.assert_not_called()
