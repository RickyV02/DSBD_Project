import time
import threading

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state = 'CLOSED'
        self.last_failure_time = 0
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):

        with self.lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                    print("CircuitBreaker: HALF_OPEN - Tentativo di ripristino...", flush=True)
                else:
                    raise CircuitBreakerOpenException("CircuitBreaker is OPEN")

        try:
            result = func(*args, **kwargs)

            with self.lock:
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                    self.failures = 0
                    print("CircuitBreaker: CLOSED - Servizio ripristinato.", flush=True)
                elif self.state == 'CLOSED':
                    # Strategy: Consecutive Failures.
                    # A successful request indicates the service is healthy, so we reset
                    # the failure count to 0 to avoid accumulating sporadic errors over long periods.
                    self.failures = 0
            return result

        except Exception as e:
            with self.lock:
                self.failures += 1
                self.last_failure_time = time.time()

                if self.failures >= self.failure_threshold:
                    self.state = 'OPEN'
                    print(f"CircuitBreaker: OPEN - Troppi errori ({self.failures}).", flush=True)

            raise e
