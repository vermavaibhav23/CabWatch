# Kafka cutover for the existing FastAPI Render service

Existing URL: https://ev-charger-guide.onrender.com/cab-watch/
Repository: https://github.com/vermavaibhav23/CabWatch (main)

This is the original Python service, not CabWatch-Java. No new paid resources are created by this repository.

1. Keep the current DATABASE_URL, database CA and optional Gemini settings. Back up the database. Deploy main with TELEMETRY_MODE=direct first; migrations are additive.
2. Provision or reuse a reachable Kafka broker and persistent standalone Redis. Supply the following environment values in the existing Render service (never commit secrets):

| Variable | Value / purpose |
| --- | --- |
| TELEMETRY_MODE | kafka, only after all dependencies are ready |
| KAFKA_BOOTSTRAP_SERVERS | Existing broker host:port list |
| KAFKA_SECURITY_PROTOCOL | SASL_SSL or SSL for a hosted broker |
| KAFKA_SSL_CA_PEM | Broker CA if not publicly trusted |
| KAFKA_SASL_MECHANISM | Provider mechanism, normally PLAIN or SCRAM-SHA-256/512 |
| KAFKA_USERNAME / KAFKA_PASSWORD | Existing broker credentials for SASL |
| REDIS_URL | Authenticated redis:// or rediss:// endpoint |
| METRICS_TOKEN | Random monitoring token |
| KAFKA_TOPIC | cabwatch.gps.recovery.v1; use a fresh topic for this schema |
| KAFKA_CREATE_TOPIC | false if an administrator created the topic |
| KAFKA_PARTITIONS | 12 for auto-creation; existing topic is not changed |
| KAFKA_REPLICATION_FACTOR | Match broker capacity; use 3 on a suitable production cluster |
| LIVE_CONSUMERS / HISTORY_CONSUMERS | 2 each initially; total across replicas <= partitions |
| LIVE_STALE_MS | 10000 initially |

3. Configure Kafka retention/storage for outages. Use replication and min.insync.replicas appropriate to the hosted cluster. The local single-broker configuration is not a production HA prescription. Configure Redis persistence/noeviction as described in README. Do not use a cache tier that can silently evict state or lose pending SQL events.
4. Pause GPS senders. Drain any existing direct requests, then restart all replicas in Kafka mode. Avoid mixed direct/Kafka processes during rollout. Reload existing dashboards so they use the asynchronous response format. Existing open legacy incidents are historical until fresh Kafka confirmation establishes the current state.
5. Check /healthz: version kafka-recovery-v1 and telemetry_mode kafka. Test one trip: 202 queued, live sequence advances, three fresh deviations open an incident, stale readings show delayed, and history includes them. Confirm metrics and SQL outbox catch-up. Verify across a restart before relying on it.
6. For rollback, pause GPS; drain BOTH consumer groups and the Redis SQL outbox before switching all replicas to direct. Restart dashboards. Do not revert the database migration or delete broker/Redis data. A rollback while work is queued can leave history or incidents unapplied.

A GitHub push alone only changes the live service if its Render auto-deploy is enabled. Verify the deployed mode; a successful direct-mode deployment does not mean Kafka is active. New infrastructure can incur charges and requires an explicit hosting decision.
