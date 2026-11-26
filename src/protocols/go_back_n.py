import time
import socket
import threading
from rich import print
from src.core.congestion_control import TCPReno
from src.core.packet import Packet, FLAG_DATA, FLAG_ACK, FLAG_FIN


class GBNSender:
    def __init__(self, sock, dest_addr, window_size=8, timeout_duration=0.5):
        self.socket = sock
        self.dest_addr = dest_addr
        self.congestion = TCPReno()
        self.rwnd = window_size
        self.timeout_duration = timeout_duration

        self.base = 0
        self.next_seqnum = 0
        self.buffer = []
        self.timer = None
        self.lock = threading.Lock()
        self.running = True

        self.ack_thread = threading.Thread(target=self._listen_for_acks, daemon=True)
        self.ack_thread.start()

    def _effective_window(self):
        return max(1, min(self.congestion.get_window(), self.rwnd))

    def send_data(self, payload):
        while True:
            with self.lock:
                if len(self.buffer) < self._effective_window():
                    pkt = Packet(seq_num=self.next_seqnum,
                                 ack_num=0, window_size=0, flags=FLAG_DATA, payload=payload)
                    self.buffer.append(pkt)
                    print(f'[bold white]SENDER:[/bold white] Sending packet {self.next_seqnum}')
                    self.socket.sendto(pkt.to_bytes(), self.dest_addr)

                    if self.base == self.next_seqnum:
                        self._start_timer()

                    self.next_seqnum += 1
                    return

            time.sleep(0.005)

    def _listen_for_acks(self):
        self.socket.set_timeout(0.2)
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
                unacked_seqs = [pkt.header.seq_num for pkt in self.buffer]

                if ack_num in unacked_seqs:
                    print(f'[bold green]SENDER:[/bold green] Received cumulative ACK {ack_num} (rwnd={self.rwnd})')

                    while self.buffer:
                        popped = self.buffer.pop(0)
                        if popped.header.seq_num == ack_num:
                            break

                    self.base = ack_num + 1
                    self.congestion.on_new_ack()
                    self._stop_timer()
                    if self.buffer:
                        self._start_timer()

                elif self.buffer and ack_num == self.base - 1:
                    if self.congestion.on_duplicate_ack():
                        print(f'[bold magenta]SENDER:[/bold magenta] Three Dup ACKs! Fast retransmitting {self.buffer[0].header.seq_num}')
                        self.socket.sendto(self.buffer[0].to_bytes(), self.dest_addr)
                        self._start_timer()

    def _on_timeout(self):
        with self.lock:
            if not self.buffer:
                return

            print(f'[bold yellow]SENDER:[/bold yellow] Timeout! Retransmitting window...')
            self._start_timer()
            for pkt in self.buffer:
                print(f'[bold yellow]\t> SENDER:[/bold yellow] Retransmitting packet {pkt.header.seq_num}')
                self.congestion.on_timeout()
                self.socket.sendto(pkt.to_bytes(), self.dest_addr)

    def _start_timer(self):
        self._stop_timer()
        self.timer = threading.Timer(self.timeout_duration, self._on_timeout)
        self.timer.daemon = True
        self.timer.start()

    def _stop_timer(self):
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None

    def close(self):
        while True:
            with self.lock:
                if not self.buffer:
                    break
            time.sleep(0.01)

        fin_pkt = Packet(seq_num=self.next_seqnum, flags=FLAG_FIN)
        self.socket.sendto(fin_pkt.to_bytes(), self.dest_addr)
        self.running = False
        self._stop_timer()



class GBNReceiver:
    def __init__(self, sock, output_path, max_buffer_packets=16):
        self.socket = sock
        self.output_path = output_path
        self.max_buffer_packets = max_buffer_packets
        self.expected_seqnum = 0

    def start(self):
        with open(self.output_path, 'wb') as file:
            while True:
                raw_data, sender_addr = self.socket.recvfrom(4096)
                try:
                    pkt = Packet.from_bytes(raw_data)
                except ValueError:
                    continue

                if not pkt.is_valid():
                    print(f'[bold red]RECEIVER:[/bold red] Dropped corrupted packet.')
                    self._send_duplicate_ack(sender_addr)
                    continue

                if pkt.header.flags & FLAG_FIN:
                    print('[bold cyan]RECEIVER:[/bold cyan] FIN received. Transfer complete.')
                    break

                if pkt.header.seq_num != self.expected_seqnum:
                    print(f'[bold yellow]RECEIVER:[/bold yellow] Out-of-Order packet {pkt.header.seq_num} (expected {self.expected_seqnum}.)')
                    self._send_duplicate_ack(sender_addr)
                    continue

                file.write(pkt.payload)
                ack_pkt = Packet(seq_num=0,
                                 ack_num=self.expected_seqnum, window_size=self.max_buffer_packets,
                                 flags=FLAG_ACK)

                print(f'[bold green]RECEIVER:[/bold green] Delivered {self.expected_seqnum}, sending ACK.')
                self.socket.sendto(ack_pkt.to_bytes(), sender_addr)
                self.expected_seqnum += 1

    def _send_duplicate_ack(self, sender_addr):
        if self.expected_seqnum == 0:
            return

        last_ack = self.expected_seqnum - 1
        ack_pkt = Packet(seq_num=0, 
                         ack_num=last_ack, window_size=self.max_buffer_packets, flags=FLAG_ACK)
        print(f'[bold yellow]RECEIVER:[/bold yellow] Sending duplicate ACK {last_ack}.')
        self.socket.sendto(ack_pkt.to_bytes(), sender_addr)