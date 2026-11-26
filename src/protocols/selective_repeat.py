import os
import time
import socket
import threading
from rich import print
from src.core.congestion_control import TCPReno
from src.core.packet import Packet, FLAG_DATA, FLAG_ACK, FLAG_FIN

class SRSender:
    def __init__(self, sock, dest_addr, window_size=8, timeout_duration=0.5):
        self.socket = sock
        self.dest_addr = dest_addr

        self.congestion = TCPReno()
        self.rwnd = window_size
        self.timeout_duration = timeout_duration

        self.base = 0
        self.next_seqnum = 0
        self.buffer = {}
        self.timers = {}
        self.lock = threading.Lock()
        self.running = True

        self.ack_thread = threading.Thread(target=self._listen_for_acks, daemon=True)
        self.ack_thread.start()

    def _effective_window(self):
        return max(1, min(self.congestion.get_window(), self.rwnd))

    def send_data(self, payload):
        while True:
            with self.lock:
                if self.next_seqnum < self.base + self._effective_window():
                    seq = self.next_seqnum
                    pkt = Packet(seq_num=seq, ack_num=0, window_size=0,
                                 flags=FLAG_DATA, payload=payload)
                    self.buffer[seq] = pkt
                    print(f'[bold white]SENDER:[/bold white] Sending packet {seq}')
                    self.socket.sendto(pkt.to_bytes(), self.dest_addr)
                    self._start_timer()

                    self.next_seqnum += 1
                    return
                time.sleep(0.005)

    def _listen_for_acks(self):
        while self.running:
            try:
                raw_data, _ = self.socket.recvfrom(4096)
                ack_pkt = Packet.from_bytes(raw_data)
            except (socket.timeout, ValueError, OSError):
                continue

            if not ack_pkt.is_valid():
                print(f'[bold red]SENDER:[/bold red] Corrupted ACK, ignoring...')
                continue
            
            if not (ack_pkt.header.flags & FLAG_ACK):
                continue
            
            ack_num = ack_pkt.header.ack_num
            with self.lock:
                self.rwnd = ack_pkt.header.window_size
                if ack_num in self.buffer:
                    print(f'[bold green]SENDER:[/bold green] Received ACK {ack_num} (rwnd={self.rwnd})')
                    del self.buffer[ack_num]
                    self._stop_timer(ack_num)

                    old_base = self.base
                    while self.base < self.next_seqnum and self.base not in self.buffer:
                        self.base += 1

                    if self.base > old_base:
                        self.congestion.on_new_ack()
                    elif self.base in self.buffer:
                        if self.congestion.on_duplicate_ack():
                            print(f'[bold magenta]SENDER:[/bold magenta] Three Dup ACKs! Fast retransmitting {self.base}')
                            self.socket.sendto(self.buffer[self.base].to_bytes(), self.dest_addr)
                            self._start_timer()

    def _on_timeout(self, seq_num):
        with self.lock:
            if seq_num not in self.buffer:
                return

            pkt = self.buffer[seq_num]
            print(f'[bold yellow]SENDER:[/bold yellow] Timeout for packet {seq_num}! Retransmitting...')
            self.congestion.on_timeout()
            self.socket.sendto(pkt.to_bytes(), self.dest_addr)
            self._start_timer(seq_num)

    def _start_timer(self, seq_num):
        self._stop_timer(seq_num)
        timer = threading.Timer(self.timeout_duration, self._on_timeout, args=(seq_num))
        timer.daemon = True
        timer.start()
        self.timers[seq_num] = timer

    def _stop_timer(self, seq_num):
        timer = self.timers.pop(seq_num, None)
        if timer is not None:
            timer.cancel()

    def close(self):
        while True:
            with self.lock:
                if not self.buffer:
                    break
            time.sleep(0.01)

        fin_pkt = Packet(seq_num=self.next_seqnum, flags=FLAG_FIN)
        self.socket.sendto(fin_pkt.to_bytes(), self.dest_addr)
        self.running = False
        with self.lock:
            for seq in list(self.timers.keys()):
                self._stop_timer(seq)


class SRReceiver:
    def __init__(self, sock, output_path, max_buffer_packets=16):
        self.socket = sock
        self.output_path = output_path
        self.max_buffer_packets = max_buffer_packets
        self.rcv_base = 0
        self.buffer = {}

    def _available_window(self):
        return max(0, self.max_buffer_packets - len(self.buffer))

    def send_ack(self, seq_num, sender_addr):
        ack_pkt = Packet(seq_num=0, ack_num=seq_num, window_size=self._available_window(), flags=FLAG_ACK)
        self.socket.sendto(ack_pkt.to_bytes(), sender_addr)

    def start(self):
        if os.path.exists(self.output_path):
            os.remove(self.output_path)

        with open(self.output_path, 'wb') as file:
            while True:
                raw_data, sender_addr = self.socket.recvfrom(4096)
                try:
                    pkt = Packet.from_bytes(raw_data)
                except ValueError:
                    continue

                if not pkt.is_valid():
                    print(f'[bold red]RECEIVER:[/bold red] Dropped corrupted packet.')
                    continue

                if pkt.header.flags & FLAG_FIN:
                    print(f'[bold cyan]RECEIVER:[/bold cyan] FIN received. Transfer complete.')
                    break

                seq = pkt.header.seq_num

                if seq < self.rcv_base:
                    print(f'[bold yellow]RECEIVER:[/bold yellow] Duplicate packet {seq}, re-sending ACK.')
                    self.send_ack(seq, sender_addr)
                    continue

                if seq < self.rcv_base + self.max_buffer_packets:
                    if seq not in self.buffer:
                        self.buffer[seq] = pkt.payload

                    while self.rcv_base in self.buffer:
                        file.write(self.buffer.pop(self.rcv_base))
                        print(f'[bold green]RECEIVER:[/bold green] Delivered packet {self.rcv_base}.')
                        self.rcv_base += 1

                    self.send_ack(seq, sender_addr)
                else:
                    print(f'[bold yellow]RECEIVER:[/bold yellow] Packet {seq} outside window, ignoring...')