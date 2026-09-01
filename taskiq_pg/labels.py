"""Label names the broker stamps onto delivered messages."""

# Broker row identity, so a middleware can act on the delivery it is holding.
ROW_ID_LABEL = "_tpg_row_id"
# Attempt count from the row. Deliberately not `_retries`: SmartRetryMiddleware
# owns that one, and overwriting it would reset the count of a re-kicked message.
ATTEMPTS_LABEL = "_tpg_attempts"
