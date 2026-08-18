# GitLab CI/CD for systemd deployment

The root `.gitlab-ci.yml` validates, packages, and deploys this application
directly to the UAT Linux host. It does not use Docker, Kubernetes, or k3s.

## Runner prerequisites

Use a Linux amd64 GitLab Runner compatible with the UAT host, with these
commands available:

- `python3.12`, `python3.12 -m venv`, and `pip`
- `ssh` and `scp`
- access to PyPI or the internal Python package mirror

Matching the runner and UAT operating-system architecture matters because the
release vendors native Python wheels such as `pyodbc` and `lz4`.
`scripts/install-lib.sh` creates a temporary virtualenv for pip so Debian and
Ubuntu PEP 668 protection remains enabled; it never installs into the system
Python environment.

The UAT host must provide `python3.12`, `rsync`, `curl`, Microsoft ODBC Driver
18, and the existing `self-healthy-kafka.service`. The SSH deployment user must
be able to write to `UAT_APP_DIR` and run these commands without an interactive
password:

```text
sudo systemctl restart self-healthy-kafka
sudo systemctl is-active self-healthy-kafka
sudo systemctl status self-healthy-kafka
```

## Protected GitLab variables

Configure these under **Settings > CI/CD > Variables**:

| Variable | Purpose |
| --- | --- |
| `UAT_SSH_HOST` | UAT server DNS name or IP address |
| `UAT_SSH_USER` | Linux deployment account |
| `UAT_SSH_PRIVATE_KEY` | Masked/protected SSH private key or File variable |
| `UAT_SSH_KNOWN_HOSTS` | Verified host key output or File variable |

Optional variables override the defaults in `.gitlab-ci.yml`:

| Variable | Default |
| --- | --- |
| `UAT_SSH_PORT` | `22` |
| `UAT_APP_DIR` | `/home/hadoop/andm2/snp-datahub/support-system/self-healthy-kafka` |
| `UAT_SYSTEMD_SERVICE` | `self-healthy-kafka` |
| `PIP_INDEX_URL` | Runner's normal pip index |

Keep `env/uat.env` only on the UAT host. The package and deployment script do
not include, replace, print, or back up this file outside the host.

The deploy job is manual for every branch. It serializes UAT deployments,
backs up the current application files, restarts systemd, and rolls back when
the service or `http://127.0.0.1:9108/metrics` does not become healthy within
60 seconds.
