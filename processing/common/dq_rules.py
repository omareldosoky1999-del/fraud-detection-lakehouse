"""Data-quality reference values.

These match what ingestion/schema/transaction.avsc and the real data actually
contain (verified against the sample Avro files). The old
`data_quality/expectations/transactions_suite.json` expected `SUCCESS/FAILED`
and `POS/ATM/ONLINE`, but the real values are `Successful/Rejected` and
`Withdrawal/Deposit/Transfer` -- every record would have failed that suite.
Keeping the truth in one importable module means the generator, the Spark DQ
step and any future Great Expectations suite can't drift apart again.
"""

VALID_TRANS_STATUS = {"Successful", "Rejected"}
VALID_TRANS_TYPE = {"Withdrawal", "Deposit", "Transfer"}
VALID_CURRENCY = {"USD", "EUR", "EGP"}
TRANS_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss a"  # Spark date format matching generator's "%Y-%m-%d %I:%M:%S %p"

REQUIRED_NOT_NULL = ["Trans_id", "Clt_id", "Card_id", "Dev_id", "Trans_amount", "Trans_date"]
