import time

SLOW_START = 'SLOW_START'
CONGESTION_AVOIDANCE = 'CONGESTION_AVOIDANCE'
FAST_RECOVERY = 'FAST_RECOVERY'

class TCPReno:
    def __init__(self, initial_cwnd=1.0, initial_ssthresh=64.0):
        self.cwnd = initial_cwnd
        self.ssthresh = initial_ssthresh
        self.state = SLOW_START
        self.dup_ack_count = 0

        self.start_time = time.time()
        self.history = []
        self.record()

    def record(self):
        self.history.append((
            time.time() - self.start_time,
            self.cwnd, self.ssthresh, self.state
        ))

    def get_window(self):
        return max(1, int(self.cwnd))

    def on_new_ack(self):
        self.dup_ack_count = 0

        if self.state == SLOW_START:
            self.cwnd += 1.0
            if self.cwnd >= self.ssthresh:
                self.state = CONGESTION_AVOIDANCE


        elif self.state == CONGESTION_AVOIDANCE:
            self.cwnd += 1.0 / self.cwnd

        elif self.state == FAST_RECOVERY:
            self.cwnd = self.ssthresh
            self.state = CONGESTION_AVOIDANCE

        self.record()

    def on_duplicate_ack(self):
        self.dup_ack_count += 1

        if self.state in (SLOW_START, CONGESTION_AVOIDANCE):
            if self.dup_ack_count == 3:
                self.ssthresh = max(self.cwnd / 2.0, 2.0)
                self.cwnd = self.ssthresh + 3.0
                self.state = FAST_RECOVERY
                self.record()
                return True

        elif self.state == FAST_RECOVERY:
            self.cwnd += 1.0
            self.record()

        return False

    def on_timeout(self):
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = 1.0
        self.dup_ack_count = 0
        self.state = SLOW_START
        self.record()