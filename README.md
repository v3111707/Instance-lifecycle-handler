## Overview

This is a Kubernetes-ready service that keeps an infrastructure inventory in a consistent state. It receives messages from a queue and updates records in an inventory store.

The service uses async functions for message processing and network operations, so multiple messages can be processed concurrently while the service waits for the message broker or inventory store.

The service is designed to run continuously in Kubernetes. It supports graceful shutdown when a pod receives a termination signal. The service stops accepting new messages, waits for messages that are already being processed, and then closes its connection to the message broker.

* **Graceful Kubernetes shutdown protects in-flight messages**
  The service handles `SIGTERM` and `SIGINT` to react to pod termination. It first stops the queue consumer and then waits for active processing tasks to finish before closing the broker connection. This reduces the risk of interrupted processing during deployments, pod restarts, and scaling.

* **Safe retries for temporary failures and non-idempotent operations** 
  The service retries only transport failures. For non-idempotent operations such as inserts, a selector can check whether the record was already created before retrying. This protects against duplicates when the write succeeded but the network failed while returning the response.

* **Batch writes are fast, but failed fields can still be identified** 
  The service first tries to save several fields in one operation. If the batch fails, it writes the fields separately. This allows valid fields to be saved and shows which fields failed.

* **Message retries and failed-message handling happen at a separate level** — 
  The queue layer retries unexpected processing failures. Permanent errors are not retried. Messages that still fail are rejected without requeueing, so the broker can route them to its failed queue.

* **Structured logs and metrics make message processing easier to observe** 
  Logs use structured JSON and can include the instance hostname. The service also records message counters and processing time metrics. This helps detect failures and slow processing.
