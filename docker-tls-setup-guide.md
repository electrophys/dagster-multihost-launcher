# Setting Up TLS for Remote Docker Daemons on Ubuntu

This guide walks through securing Docker daemon TCP access with mutual TLS (mTLS) across your Dagster multi-host deployment. When complete, the Dagster daemon on Host A will authenticate to Docker daemons on Hosts B, C, etc. over encrypted connections, and those daemons will reject any client without a valid certificate.

## Overview

Mutual TLS means both sides authenticate:

- The **server** (remote Docker daemon) presents a certificate so the client knows it's talking to the right host
- The **client** (Dagster daemon on Host A) presents a certificate so the server knows the request is authorized

All certificates are signed by a private Certificate Authority (CA) that you control.

```
┌─────────────────┐          TLS          ┌─────────────────┐
│  Host A          │  ◄──────────────────► │  Host B          │
│  (Dagster daemon)│                       │  (Docker daemon) │
│                  │                       │                  │
│  Presents:       │                       │  Presents:       │
│   client-cert.pem│                       │   server-cert.pem│
│                  │                       │                  │
│  Verifies:       │                       │  Verifies:       │
│   server cert    │                       │   client cert    │
│   against ca.pem │                       │   against ca.pem │
└─────────────────┘                        └─────────────────┘
```

## Prerequisites

On the machine where you'll generate certificates (can be Host A or any workstation):

```bash
sudo apt update
sudo apt install -y openssl
```

Decide on a directory to work in:

```bash
mkdir -p ~/docker-tls && cd ~/docker-tls
```

## Step 1: Create the Certificate Authority (CA)

The CA is the root of trust. Both Docker daemons and the Dagster client will trust certificates signed by this CA.

### 1.1 Generate the CA private key

```bash
openssl genrsa -aes256 -out ca-key.pem 4096
```

You'll be prompted for a passphrase. Choose something strong and store it securely — you'll need it every time you sign a new certificate.

### 1.2 Generate the CA certificate

```bash
openssl req -new -x509 -days 3650 -key ca-key.pem -sha256 -out ca.pem \
  -subj "/C=US/ST=Washington/L=Pasco/O=MyOrg/CN=Docker CA"
```

Adjust the subject fields to match your organization. The `-days 3650` gives you a 10-year CA — adjust as needed for your security policy.

You now have:
- `ca-key.pem` — CA private key (keep this secret and offline if possible)
- `ca.pem` — CA certificate (distribute to all hosts)

## Step 2: Generate Server Certificates (One Per Docker Host)

Each remote Docker host needs its own server certificate. Repeat this section for each host (B, C, etc.).

### 2.1 Create the server private key

```bash
# Replace "host-b" with a name identifying this host
openssl genrsa -out server-host-b-key.pem 4096
```

### 2.2 Create a Certificate Signing Request (CSR)

```bash
openssl req -subj "/CN=host-b" -sha256 -new \
  -key server-host-b-key.pem -out server-host-b.csr
```

### 2.3 Create a Subject Alternative Name (SAN) extensions file

This is critical — Docker validates the server certificate against the hostname or IP the client connects to. List all names and IPs the host might be reached by:

```bash
cat > server-host-b-extfile.cnf <<EOF
subjectAltName = DNS:host-b,DNS:host-b.example.com,IP:10.0.1.2,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
```

**Adjust the values:**
- `DNS:host-b` — short hostname
- `DNS:host-b.example.com` — FQDN if you use DNS
- `IP:10.0.1.2` — the IP address used in your `dagster.yaml` `docker_url`
- `IP:127.0.0.1` — for local testing on the host itself

### 2.4 Sign the server certificate

```bash
openssl x509 -req -days 1825 -sha256 \
  -in server-host-b.csr \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-host-b-cert.pem \
  -extfile server-host-b-extfile.cnf
```

Enter the CA key passphrase when prompted. This gives you a server cert valid for 5 years.

### 2.5 Clean up the CSR and extfile (optional)

```bash
rm server-host-b.csr server-host-b-extfile.cnf
```

You now have for this host:
- `server-host-b-key.pem` — server private key
- `server-host-b-cert.pem` — server certificate

### 2.6 Repeat for each additional host

```bash
# Host C example
openssl genrsa -out server-host-c-key.pem 4096

openssl req -subj "/CN=host-c" -sha256 -new \
  -key server-host-c-key.pem -out server-host-c.csr

cat > server-host-c-extfile.cnf <<EOF
subjectAltName = DNS:host-c,DNS:host-c.example.com,IP:10.0.1.3,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -days 1825 -sha256 \
  -in server-host-c.csr \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-host-c-cert.pem \
  -extfile server-host-c-extfile.cnf

rm server-host-c.csr server-host-c-extfile.cnf
```

## Step 3: Generate the Client Certificate (For Host A / Dagster)

The Dagster daemon uses a single client certificate to authenticate to all remote Docker daemons.

### 3.1 Create the client private key

```bash
openssl genrsa -out client-key.pem 4096
```

### 3.2 Create the CSR

```bash
openssl req -subj "/CN=dagster-client" -new -key client-key.pem -out client.csr
```

### 3.3 Create the extensions file

```bash
cat > client-extfile.cnf <<EOF
extendedKeyUsage = clientAuth
EOF
```

### 3.4 Sign the client certificate

```bash
openssl x509 -req -days 1825 -sha256 \
  -in client.csr \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client-cert.pem \
  -extfile client-extfile.cnf
```

### 3.5 Clean up

```bash
rm client.csr client-extfile.cnf
```

You now have:
- `client-key.pem` — client private key
- `client-cert.pem` — client certificate

## Step 4: Set File Permissions

Private keys should only be readable by root (or the user running Docker/Dagster):

```bash
chmod 0400 ca-key.pem server-*-key.pem client-key.pem
chmod 0444 ca.pem server-*-cert.pem client-cert.pem
```

## Step 5: Distribute Certificates

### To each remote Docker host (B, C, etc.)

Each host needs three files:
- `ca.pem` — to verify client certificates
- `server-<host>-cert.pem` — its own server certificate
- `server-<host>-key.pem` — its own server private key

```bash
# Copy to Host B
scp ca.pem server-host-b-cert.pem server-host-b-key.pem user@10.0.1.2:/tmp/

# Then on Host B:
ssh user@10.0.1.2 <<'EOF'
sudo mkdir -p /etc/docker/tls
sudo mv /tmp/ca.pem /etc/docker/tls/
sudo mv /tmp/server-host-b-cert.pem /etc/docker/tls/server-cert.pem
sudo mv /tmp/server-host-b-key.pem /etc/docker/tls/server-key.pem
sudo chmod 0400 /etc/docker/tls/server-key.pem
sudo chmod 0444 /etc/docker/tls/ca.pem /etc/docker/tls/server-cert.pem
sudo chown -R root:root /etc/docker/tls
EOF
```

Repeat for Host C with its own server cert/key.

### To Host A (Dagster control plane)

Host A needs three files:
- `ca.pem` — to verify server certificates
- `client-cert.pem` — to present to remote daemons
- `client-key.pem` — client private key

```bash
# On Host A, create a certs directory accessible to docker-compose
mkdir -p ~/dagster-deployment/certs
cp ca.pem client-cert.pem client-key.pem ~/dagster-deployment/certs/
chmod 0400 ~/dagster-deployment/certs/client-key.pem
```

## Step 6: Configure Docker Daemon on Remote Hosts

Perform these steps on each remote Docker host (B, C, etc.).

### 6.1 Configure daemon.json

```bash
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "tls": true,
  "tlsverify": true,
  "tlscacert": "/etc/docker/tls/ca.pem",
  "tlscert": "/etc/docker/tls/server-cert.pem",
  "tlskey": "/etc/docker/tls/server-key.pem"
}
EOF
```

Note: we do NOT put `"hosts"` in `daemon.json` because on Ubuntu with systemd, the `-H` flag in the service unit conflicts with the `hosts` key in `daemon.json`. Instead, we configure the listening addresses via the systemd override.

### 6.2 Create a systemd override

```bash
sudo systemctl edit docker.service
```

This opens an editor. Add the following:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd -H fd:// -H tcp://0.0.0.0:2376
```

The first empty `ExecStart=` clears the default command. The second line adds both the default socket (`fd://` for systemd socket activation) and the TCP listener.

### 6.3 Restart Docker

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

### 6.4 Verify it's listening

```bash
sudo ss -tlnp | grep 2376
```

You should see:

```
LISTEN  0  4096  *:2376  *:*  users:(("dockerd",pid=...,fd=...))
```

### 6.5 Open the firewall (if using ufw)

```bash
# Only allow Host A to connect
sudo ufw allow from 10.0.1.1 to any port 2376 proto tcp comment "Docker TLS from Dagster"
```

Or if using iptables directly:

```bash
sudo iptables -A INPUT -p tcp --dport 2376 -s 10.0.1.1 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 2376 -j DROP
```

## Step 7: Test the Connection

### From Host A (command line)

```bash
# Test with the Docker CLI
docker \
  --tlsverify \
  --tlscacert=~/dagster-deployment/certs/ca.pem \
  --tlscert=~/dagster-deployment/certs/client-cert.pem \
  --tlskey=~/dagster-deployment/certs/client-key.pem \
  -H=tcp://10.0.1.2:2376 \
  info
```

You should see Docker system info for Host B. If you get a TLS handshake error, check:
- The IP/hostname matches a SAN entry in the server cert
- The CA cert is the same one that signed both client and server certs
- File permissions are correct

### From Host A (Python — same as the launcher uses)

```bash
pip install docker
```

```python
import docker
from docker.tls import TLSConfig

tls = TLSConfig(
    ca_cert="/home/you/dagster-deployment/certs/ca.pem",
    client_cert=(
        "/home/you/dagster-deployment/certs/client-cert.pem",
        "/home/you/dagster-deployment/certs/client-key.pem",
    ),
    verify=True,
)

client = docker.DockerClient(base_url="tcp://10.0.1.2:2376", tls=tls)
print(client.info()["Name"])  # Should print Host B's hostname
print(client.containers.list())
```

### Verify mutual auth is enforced

From any other machine (without the client cert), a connection should be refused:

```bash
curl https://10.0.1.2:2376/version
# Should fail with SSL handshake error
```

## Step 8: Wire Into dagster.yaml

Now reference the certs in your Dagster configuration. The paths below are as seen from inside the Dagster daemon/webserver containers (mounted via docker-compose volumes).

```yaml
run_launcher:
  module: dagster_multihost_launcher
  class: MultiHostDockerRunLauncher
  config:
    docker_hosts:
      - host_name: "host-a"
        docker_url: "unix:///var/run/docker.sock"
        location_names:
          - "local_pipelines"
        network: "dagster_network"
        # No TLS needed for the local socket

      - host_name: "host-b"
        docker_url: "tcp://10.0.1.2:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
          verify: true
        location_names:
          - "etl_pipelines"
        network: "host_b_dagster_network"

      - host_name: "host-c"
        docker_url: "tcp://10.0.1.3:2376"
        tls:
          ca_cert: "/certs/ca.pem"
          client_cert: "/certs/client-cert.pem"
          client_key: "/certs/client-key.pem"
          verify: true
        location_names:
          - "analytics"
        network: "host_c_dagster_network"
```

And the docker-compose volume mount on Host A:

```yaml
  daemon:
    volumes:
      - ./certs:/certs:ro
      - /var/run/docker.sock:/var/run/docker.sock
      # ...
```

## Automation Script

For convenience, here's a script that generates the full set of certificates. Save it as `generate-docker-tls.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Configuration — edit these
# ============================================================

CA_SUBJ="/C=US/ST=Washington/L=Pasco/O=MyOrg/CN=Docker CA"
CA_DAYS=3650              # CA validity: 10 years
CERT_DAYS=1825            # Cert validity: 5 years
OUTPUT_DIR="./docker-tls"

# Define your hosts: "name|ip|dns1,dns2,..."
# DNS entries are optional. IP is required.
DOCKER_HOSTS=(
  "host-b|10.0.1.2|host-b,host-b.example.com"
  "host-c|10.0.1.3|host-c,host-c.example.com"
)

# ============================================================
# Script
# ============================================================

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo "=== Generating CA ==="
openssl genrsa -aes256 -out ca-key.pem 4096
openssl req -new -x509 -days "$CA_DAYS" -key ca-key.pem -sha256 \
  -out ca.pem -subj "$CA_SUBJ"

echo ""
echo "=== Generating Client Certificate ==="
openssl genrsa -out client-key.pem 4096
openssl req -subj "/CN=dagster-client" -new \
  -key client-key.pem -out client.csr

cat > client-extfile.cnf <<EOF
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -days "$CERT_DAYS" -sha256 \
  -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client-cert.pem -extfile client-extfile.cnf
rm client.csr client-extfile.cnf

for host_entry in "${DOCKER_HOSTS[@]}"; do
  IFS='|' read -r name ip dns_list <<< "$host_entry"

  echo ""
  echo "=== Generating Server Certificate for $name ($ip) ==="

  openssl genrsa -out "server-${name}-key.pem" 4096
  openssl req -subj "/CN=${name}" -sha256 -new \
    -key "server-${name}-key.pem" -out "server-${name}.csr"

  # Build SAN string
  san="IP:${ip},IP:127.0.0.1"
  if [ -n "$dns_list" ]; then
    IFS=',' read -ra dns_entries <<< "$dns_list"
    for dns in "${dns_entries[@]}"; do
      san="${san},DNS:${dns}"
    done
  fi

  cat > "server-${name}-extfile.cnf" <<EOF
subjectAltName = ${san}
extendedKeyUsage = serverAuth
EOF

  openssl x509 -req -days "$CERT_DAYS" -sha256 \
    -in "server-${name}.csr" \
    -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -out "server-${name}-cert.pem" \
    -extfile "server-${name}-extfile.cnf"

  rm "server-${name}.csr" "server-${name}-extfile.cnf"
done

# Set permissions
chmod 0400 ca-key.pem *-key.pem
chmod 0444 ca.pem *-cert.pem

echo ""
echo "=== Done ==="
echo ""
echo "Generated files in ${OUTPUT_DIR}/:"
ls -la
echo ""
echo "Next steps:"
echo "  1. Copy ca.pem + server-<host>-cert.pem + server-<host>-key.pem to each Docker host"
echo "  2. Copy ca.pem + client-cert.pem + client-key.pem to Host A (Dagster)"
echo "  3. Configure Docker daemons (see guide)"
echo "  4. Store ca-key.pem somewhere safe and offline"
```

Make it executable and run:

```bash
chmod +x generate-docker-tls.sh
./generate-docker-tls.sh
```

## Certificate Renewal

Certificates expire. Plan for renewal before they do:

- **Track expiry dates.** Check a certificate's expiry with:
  ```bash
  openssl x509 -in server-host-b-cert.pem -noout -enddate
  ```

- **To renew**, generate a new CSR with the same key (or a new key) and re-sign with the CA. Distribute the new cert and restart Docker. No CA change needed unless the CA itself is expiring.

- **Consider automation.** For larger deployments, tools like HashiCorp Vault, step-ca (smallstep), or cfssl can automate certificate issuance and renewal.

## Troubleshooting

**"certificate signed by unknown authority"**
→ The ca.pem on the verifying side doesn't match the CA that signed the cert. Make sure the same ca.pem is on both Host A and the remote hosts.

**"remote error: tls: bad certificate"**
→ The client cert wasn't signed by the CA the server trusts, or the client cert has `extendedKeyUsage = serverAuth` instead of `clientAuth` (or vice versa).

**"x509: certificate is valid for X, not Y"**
→ The hostname or IP used in `docker_url` doesn't match any SAN entry in the server cert. Re-generate the server cert with the correct SANs.

**"connection refused" on port 2376**
→ Docker isn't listening on TCP. Check `sudo ss -tlnp | grep 2376` and verify the systemd override is in place.

**"context deadline exceeded" or timeouts**
→ Firewall is blocking port 2376. Check ufw/iptables rules. Test with `nc -zv 10.0.1.2 2376`.

**Docker won't start after config changes**
→ Common cause: both `daemon.json` and the systemd unit specify `-H` flags, which conflict. Use `daemon.json` for TLS settings and the systemd override for host bindings, or put everything in one place.

Check logs with:
```bash
sudo journalctl -u docker.service --no-pager -n 50
```
