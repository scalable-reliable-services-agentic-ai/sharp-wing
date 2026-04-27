INSERT INTO transactions (transaction_id, client_id, receiver_id, timestamp_iso, transaction_type, \
                                              channel, amount, currency, location, ip_address, mac_address, \
                                              fingerprint, session_id, timestamp_ms, current_state, history, \
                                              client_type, is_fraud, fraud_reason) \
                    VALUES %s