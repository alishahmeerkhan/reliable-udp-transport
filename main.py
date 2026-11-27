import os
import time
import hashlib
import argparse
import threading
from rich import print
import matplotlib.pyplot as plt

from src.network.impairment import ImpairedSocket
from src.protocols.go_back_n import GBNSender, GBNReceiver
from src.protocols.selective_repeat import SRSender, SRReceiver


def sha256_file(file_path):
    hash = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(8192), b''):
            hash.update(chunk)

    return hash.hexdigest()


def ensure_sample_file(file_path, size_kb=100):
    dir_name = os.path.dirname(file_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name)

    if os.path.exists(file_path):
        os.remove(file_path)

    if not os.path.exists(file_path):
        print(f'[bold cyan]INFO:[/bold cyan] Creating {size_kb}KB sample file at {file_path}')

        with open(file_path, 'wb') as file:
            file.write(os.urandom(size_kb * 1024))

def plot_cwnd(history, protocol, loss_prob, output_path='data/graph/cwnd_plot.png'):
    dir_name = os.path.dirname(output_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name)

    if os.path.exists(output_path):
        os.remove(output_path)

    if not os.path.exists(output_path):
        times = [entry[0] for entry in history]
        cwnds = [entry[1] for entry in history]
        ssthreshs = [entry[2] for entry in history]

        plt.figure(figsize=(10,5))
        plt.plot(times, cwnds, label='cwnd (packets)', linewidth=2)
        plt.plot(times, ssthreshs, label='ssthresh', linestyle='--', alpha=0.7)

        plt.title(f'TCP Reno Congestion Control ({protocol.upper()} | Loss: {loss_prob * 100:.0f}%)')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Window Size (packets)')

        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path)
        print(f'\n\n[bold green]PLOT:[/bold green] Saved cwnd graph to {output_path}')
        plt.show()

def run_transfer(args):
    ensure_sample_file(args.input)

    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    host = '127.0.0.1'
    sender_addr = (host, args.sender_port)
    receiver_addr = (host, args.receiver_port)

    sender_sock = ImpairedSocket(loss_prob=args.loss, corrupt_prob=args.corrupt, delay_prob=args.delay)
    sender_sock.bind(sender_addr)

    receiver_sock = ImpairedSocket()
    receiver_sock.bind(receiver_addr)

    if args.protocol == 'gbn':
        receiver = GBNReceiver(receiver_sock, args.output, max_buffer_packets=args.window)
        sender = GBNSender(sender_sock, receiver_addr, window_size=args.window, timeout_duration=args.timeout)
    else:
        receiver = SRReceiver(receiver_sock, args.output, max_buffer_packets=args.window)
        sender = SRSender(sender_sock, receiver_addr, window_size=args.window, timeout_duration=args.timeout)

    rcv_thread = threading.Thread(target=receiver.start, daemon=True)
    rcv_thread.start()

    file_size = os.path.getsize(args.input)
    print(f'\n[bold cyan] --- Starting {args.protocol.upper()} Transfer ---[/bold cyan]')
    print(f'File: {args.input} ({file_size} bytes) | Loss: {args.loss} | Corrupt: {args.corrupt} | Delay: {args.delay}', end='\n\n')

    start_time = time.time()

    with open(args.input, 'rb') as file:
        while True:
            chunk = file.read(args.chunk_size)
            if not chunk:
                break
            sender.send_data(chunk)

    sender_sock.loss_prob = 0.0
    sender_sock.corrupt_prob = 0.0
    sender.close()

    rcv_thread.join(timeout=5.0)
    elapsed_time = time.time() - start_time

    sender_sock.close()
    receiver_sock.close()

    in_hash = sha256_file(args.input)
    out_hash = sha256_file(args.output)
    throughput_kbps = (file_size / 1024.0) / elapsed_time if elapsed_time > 0 else 0


    print('\n\n[bold italic cyan] --- Transfer Summary ---[/bold italic cyan]')
    print(f'Elapsed Time:  {elapsed_time:.6f} s')
    print(f'Througput:     {throughput_kbps:.6f} KB/s')
    print(f'Input SHA256:  {in_hash}')
    print(f'Output SHA256: {out_hash}') 

    if in_hash == out_hash:
        print(f'\n[bold green]STATUS: SUCCESS![/bold green]')
    else:
        print(f'[bold red]STATUS: FAILED![/bold red]')

    if args.plot:
        plot_cwnd(sender.congestion.history, args.protocol, args.loss)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reliable UDP Transport (GBN / SR + TCP Reno)')

    parser.add_argument('--protocol', choices=['gbn', 'sr'], default='gbn', help='Protocol to use (gbn or sr)')
    parser.add_argument('--input', default='data/input/sample.bin', help="Path to input file")
    parser.add_argument("--output", default='data/output/received.bin', help="Path to output file")
    parser.add_argument("--loss", type=float, default=0.05, help="Packet loss probability (0.0 - 1.0)")
    parser.add_argument("--corrupt", type=float, default=0.02, help="Packet bit-corruption probability (0.0 - 1.0)")
    parser.add_argument("--delay", type=float, default=0.0, help="Packet delay probability (0.0 - 1.0)")
    parser.add_argument("--window", type=int, default=16, help="Receiver max buffer window (packets)")
    parser.add_argument("--chunk-size", type=int, default=1024, help="Payload bytes per packet")
    parser.add_argument("--timeout", type=float, default=0.5, help="Retransmission timeout in seconds")
    parser.add_argument("--sender-port", type=int, default=5000, help="Sender UDP port")
    parser.add_argument("--receiver-port", type=int, default=5001, help="Receiver UDP port")
    parser.add_argument("--plot", action="store_true", help="Plot TCP Reno cwnd over time")

    run_transfer(parser.parse_args())