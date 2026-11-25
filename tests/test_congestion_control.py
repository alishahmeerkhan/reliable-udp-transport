import pytest

from src.core.congestion_control import (
    TCPReno,
    SLOW_START,
    CONGESTION_AVOIDANCE,
    FAST_RECOVERY
)

def test_initialization():
    tcp = TCPReno(initial_cwnd=1.0, initial_ssthresh=64.0)

    assert tcp.cwnd == 1.0
    assert tcp.ssthresh == 64.0
    assert tcp.state == SLOW_START
    assert tcp.dup_ack_count == 0

    assert len(tcp.history) == 1

def test_get_window():
    tcp = TCPReno(initial_cwnd=5.8)
    assert tcp.get_window() == 5
    tcp.cwnd = 0.5

    assert tcp.get_window() == 1

def test_slow_start():
    tcp = TCPReno(initial_cwnd=1.0, initial_ssthresh=10.0)

    tcp.on_new_ack()

    assert tcp.cwnd == 2.0
    assert tcp.state == SLOW_START

    tcp.on_new_ack()

    assert tcp.cwnd == 3.0
    assert tcp.state == SLOW_START

def test_slow_start_to_congestion_avoidance():
    tcp = TCPReno(initial_cwnd=3.0, initial_ssthresh=4.0)

    tcp.on_new_ack()

    assert tcp.cwnd == 4.0
    assert tcp.state == CONGESTION_AVOIDANCE

def test_congestion_avoidance():
    tcp = TCPReno(initial_cwnd=10.0, initial_ssthresh=10.0)

    assert tcp.state == SLOW_START
    tcp.on_new_ack()

    assert tcp.state == CONGESTION_AVOIDANCE
    assert tcp.cwnd == 11.0

    old_cwnd = tcp.cwnd

    tcp.on_new_ack()

    assert tcp.cwnd == pytest.approx(
        old_cwnd + (1.0 / old_cwnd)
    )

def test_duplicate_ack_count():
    tcp = TCPReno()

    assert tcp.on_duplicate_ack() is False
    assert tcp.dup_ack_count == 1

    assert tcp.on_duplicate_ack() is False
    assert tcp.dup_ack_count == 2

def test_fast_recovery():
    tcp = TCPReno(initial_cwnd=10.0, initial_ssthresh=64.0)

    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()

    result = tcp.on_duplicate_ack()

    assert result is True

    assert tcp.ssthresh == 5.0
    assert tcp.cwnd == 8.0
    assert tcp.state == FAST_RECOVERY
    assert tcp.dup_ack_count == 3

def test_duplicate_ack_during_fast_recovery():
    tcp = TCPReno(initial_cwnd=10.0)
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()

    old_cwnd = tcp.cwnd

    result = tcp.on_duplicate_ack()

    assert result is False
    assert tcp.cwnd == old_cwnd + 1.0
    assert tcp.state == FAST_RECOVERY

def test_new_ack_during_fast_recovery():
    tcp = TCPReno(initial_cwnd=10.0)

    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()

    assert tcp.state == FAST_RECOVERY

    tcp.on_new_ack()

    assert tcp.cwnd == tcp.ssthresh
    assert tcp.state == CONGESTION_AVOIDANCE
    assert tcp.dup_ack_count == 0

def test_timeout():
    tcp = TCPReno(initial_cwnd=16.0, initial_ssthresh=64.0)

    tcp.on_timeout()

    assert tcp.ssthresh == 8.0
    assert tcp.cwnd == 1.0
    assert tcp.dup_ack_count == 0
    assert tcp.state == SLOW_START

def test_minimum_ssthresh():
    tcp = TCPReno(initial_cwnd=2.0)

    tcp.on_timeout()

    assert tcp.ssthresh == 2.0

    tcp = TCPReno(initial_cwnd=3.0)

    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()

    assert tcp.ssthresh == 2.0

def test_history_recording():
    tcp = TCPReno()
    
    initial_length = len(tcp.history)

    tcp.on_new_ack()
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()
    tcp.on_duplicate_ack()
    tcp.on_timeout()

    assert len(tcp.history) == initial_length + 3

    for entry in tcp.history:
        timestamp, cwnd, ssthresh, state = entry

        assert timestamp >= 0
        assert cwnd > 0
        assert ssthresh > 0

        assert state in (
            SLOW_START,
            CONGESTION_AVOIDANCE,
            FAST_RECOVERY
        )
