# Running tests for Firebolt Engine over HTTPS

The integration workflow brings up nginx as a reverse proxy for HTTPS coverage. It generates a one-day self-signed certificate for `localhost`, adds the certificate to the runner's trust store, and mounts the certificate and private key into nginx.

The client connects to nginx over HTTPS. nginx terminates TLS and forwards requests to Firebolt Engine over HTTP on port 3473.
