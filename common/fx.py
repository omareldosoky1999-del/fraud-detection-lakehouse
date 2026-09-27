"""Shared FX helper used by the generator AND the Spark jobs.

The rates are STATIC and illustrative (a real system would read them from a
rates service). Keeping them in one place guarantees that the generator, the
rules and the evaluation all agree on what "USD-equivalent" means.
"""

FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "EGP": 0.02}


def to_usd(amount, currency):
    """Convert an amount in `currency` to USD (unknown currency -> treated as USD)."""
    return float(amount) * FX_TO_USD.get(currency, 1.0)
