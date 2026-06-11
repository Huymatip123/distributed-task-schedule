"""
tasks.py
--------
Implements the CPU-intensive task types suggested in Section 5 of the
assignment:

  - prime_count   (Task A: Prime Counting)
  - matrix_mult   (Task B: Matrix Multiplication)
  - monte_carlo_pi (Task C: Monte Carlo Pi)
  - word_count    (Task D: Word Count)
  - factorial     (bonus / warm-up task)

run_task(task_type, input_data) is the single entry point used by worker.py.
"""

import random


def run_task(task_type, input_data):
    if task_type == "prime_count":
        return prime_count(int(input_data))
    elif task_type == "matrix_mult":
        return matrix_mult(int(input_data))
    elif task_type == "monte_carlo_pi":
        return monte_carlo_pi(int(input_data))
    elif task_type == "word_count":
        return word_count(int(input_data))
    elif task_type == "factorial":
        return factorial(int(input_data))
    else:
        raise ValueError("Unknown task type: %s" % task_type)


def prime_count(n):
    """Count primes <= n using a simple sieve of Eratosthenes."""
    if n < 2:
        return 0
    sieve = bytearray([1]) * (n + 1)
    sieve[0] = sieve[1] = 0
    i = 2
    while i * i <= n:
        if sieve[i]:
            sieve[i * i:: i] = bytearray(len(sieve[i * i:: i]))
        i += 1
    return sum(sieve)


def matrix_mult(size):
    """Multiply two `size`x`size` matrices of random floats."""
    A = [[random.random() for _ in range(size)] for _ in range(size)]
    B = [[random.random() for _ in range(size)] for _ in range(size)]
    C = [[0.0] * size for _ in range(size)]
    for i in range(size):
        Ai = A[i]
        Ci = C[i]
        for k in range(size):
            a = Ai[k]
            if a == 0.0:
                continue
            Bk = B[k]
            for j in range(size):
                Ci[j] += a * Bk[j]
    return "%dx%d matrix multiplication done, C[0][0]=%.4f" % (size, size, C[0][0])


def monte_carlo_pi(samples):
    """Estimate pi by random sampling of the unit square / unit circle."""
    inside = 0
    for _ in range(samples):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return 4.0 * inside / samples


def word_count(num_words):
    """Generate a random 'document' of num_words words and return word
    frequencies (Task D)."""
    vocab = ["alpha", "beta", "gamma", "delta", "epsilon",
             "zeta", "eta", "theta", "iota", "kappa"]
    counts = {}
    for _ in range(num_words):
        w = random.choice(vocab)
        counts[w] = counts.get(w, 0) + 1
    return counts


def factorial(n):
    """Compute n! and return its number of digits (avoids huge JSON output)."""
    result = 1
    for i in range(2, n + 1):
        result *= i
    return "%d! has %d digits" % (n, len(str(result)))
