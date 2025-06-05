# Running tests for firebolt core over https

In order to run the integration tests using https protocol, we need to bring up an nginx reverse proxy. On this proxy we will install the server certificate. In our case it will be a self signed cert (localhost.pem) and its private key (localhost-key.pem). 

The cert and its key will be stored in GitHub since we cannot have secrets stored in code repository, and it would be easier to rotate them (no code changes required):
- FIREBOLT_CORE_DEV_CERT is the certificate
- FIREBOLT_CORE_DEV_CERT_PRIVATE_KEY is the private key

This is the certificate information that we currently have. Note that it wille expire in 2 years (when it will expire we will generate a new certificate and key and set them in the Git repository secrets)

Certificate Information:
- Common Name: 
- Subject Alternative Names: localhost
- Organization: mkcert development certificate
- Organization Unit: bogdantruta@Mac.localdomain (Bogdan Truta)
- Locality: 
- State: 
- Country: 
- Valid From: June 5, 2025
- Valid To: September 5, 2027
- Issuer: mkcert bogdantruta@Mac.localdomain (Bogdan Truta), mkcert development CA
- Key Size: 2048 bit
- Serial Number: 50f4b4c2a68d9525064b09f1f6251e62


The client will connect over https to the nginx server which will do the TLS handshake and the TLS termination. It will then call the firebolt-core over http on port 3473.

