# AGNT5 serverless Go endpoint

This example exposes a Go workflow through the AGNT5 `workerless.v1` HTTP
protocol without a persistent worker process.

```bash
export AGNT5_SERVERLESS_SIGNING_SECRET="$(openssl rand -base64 32)"
go run ./examples/serverless-http
```

Validate the endpoint in another terminal:

```bash
agnt5 serverless validate http://127.0.0.1:8787
```

Deploy the HTTP server to an HTTPS host, then sync its immutable release:

```bash
agnt5 serverless sync https://<go-host> \
  --provider node \
  --immutable-ref <git-sha-or-release-id> \
  --signing-secret-env AGNT5_SERVERLESS_SIGNING_SECRET \
  --activate=false
```
