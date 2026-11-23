import pytest
from unittest.mock import MagicMock, patch
from src.network.impairment import ImpairedSocket


def test_successful_send():
    imp_socket = ImpairedSocket(loss_prob=0.0, corrupt_prob=0.0, delay_prob=0.0)
    imp_socket.close()
    imp_socket.socket = MagicMock()

    payload = b"Hello"
    addr = ("127.0.0.1", 5000)

    imp_socket.sendto(payload, addr)

    imp_socket.socket.sendto.assert_called_once_with(payload, addr)


def test_closed_socket():
    imp_socket = ImpairedSocket(min_delay=0.0, max_delay=0.0)
    imp_socket.close()

    imp_socket.delayed_send(b"Hello", ("127.0.0.1", 5000))


def test_packet_loss():
    imp_socket = ImpairedSocket(loss_prob=1.0, corrupt_prob=0.0, delay_prob=0.0)
    imp_socket.close()
    imp_socket.socket = MagicMock()

    payload = b"Hello"
    addr = ("127.0.0.1", 5000)

    imp_socket.sendto(payload, addr)

    imp_socket.socket.sendto.assert_not_called()


def test_packet_corruption():
    imp_socket = ImpairedSocket(loss_prob=0.0, corrupt_prob=1.0, delay_prob=0.0)
    imp_socket.close()
    imp_socket.socket = MagicMock()

    payload = b"Hello"
    addr = ("127.0.0.1", 5000)

    imp_socket.sendto(payload, addr)

    imp_socket.socket.sendto.assert_called_once()
    sent_payload, sent_addr = imp_socket.socket.sendto.call_args[0]

    assert sent_addr == addr
    assert len(sent_payload) == len(payload)
    assert sent_payload != payload


def test_packet_delay():
    imp_socket = ImpairedSocket(loss_prob=0.0, corrupt_prob=0.0, delay_prob=1.0)
    imp_socket.close()
    imp_socket.socket = MagicMock()

    payload = b"Hello"
    addr = ("127.0.0.1", 5000)

    with patch("src.network.impairment.random.random", side_effect=[0.5, 0.5, 0.0]), \
         patch("src.network.impairment.threading.Thread") as mock_thread:
        imp_socket.sendto(payload, addr)

        mock_thread.assert_called_once_with(
            target=imp_socket.delayed_send,
            args=(payload, addr),
            daemon=True,
        )
        mock_thread.return_value.start.assert_called_once()
        imp_socket.socket.sendto.assert_not_called()