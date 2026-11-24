import os
import ssl

def options():
    result = {"bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    result["security_protocol"] = protocol
    if protocol in ("SSL", "SASL_SSL"):
        result["ssl_context"] = ssl.create_default_context(cadata=os.getenv("KAFKA_SSL_CA_PEM"))
    if protocol.startswith("SASL"):
        result.update(sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.environ["KAFKA_USERNAME"], sasl_plain_password=os.environ["KAFKA_PASSWORD"])
    return result

