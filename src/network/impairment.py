from rich import print
import socket, random, threading, time

class ImpairedSocket:
    def __init__(self, loss_prob=0.0, corrupt_prob=0.0, delay_prob=0.0, min_delay=0.05, max_delay=0.25):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.loss_prob = loss_prob
        self.corrupt_prob = corrupt_prob
        self.delay_prob = delay_prob
        self.min_delay = min_delay
        self.max_delay = max_delay

    def bind(self, addr):
        self.socket.bind(addr)

    def set_timeout(self, timeout):
        self.socket.settimeout(timeout)

    def recvfrom(self, buffer_size=4096):
        return self.socket.recvfrom(buffer_size)

    def close(self):
        self.socket.close()

    def corrupt(self, payload):
        if not payload:
            return payload

        mutable = bytearray(payload)
        index = random.randint(0, len(mutable) - 1)
        mutable[index] ^= (1 << random.randint(0, 7))
        return bytes(mutable)

    def delayed_send(self, payload, addr):
        time.sleep(random.uniform(self.min_delay, self.max_delay))

        try:
            self.socket.sendto(payload, addr)
        except OSError:
            print('[bold red]Connection has been closed.[/bold red] Unable to send recent payload.')

    def sendto(self, payload, addr):
        if random.random() < self.loss_prob:
            print('[bold red]LOG: Packet dropped in channel.[/bold red]')
            return

        if random.random() < self.corrupt_prob:
            print('[bold yellow]LOG: Packet bit-corrupted in channel.[/bold yellow]')
            payload = self.corrupt(payload=payload)

        if random.random() < self.delay_prob:
            print('[bold red]LOG: Packet delayed in channel.[/bold red]')
            threading.Thread(target=self.delayed_send, args=(payload, addr), daemon=True).start()
            return

        self.socket.sendto(payload, addr)