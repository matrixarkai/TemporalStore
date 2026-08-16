# Deploying the centralized config file

TemporalStore / MatrixArk services read every tunable from one documented file,
`config/temporalstore.toml`, via a non-invasive launcher (`scripts/with_config.sh`
+ `tools/matrixark_load_config.py`). This directory wires that file into the
real systemd deployment.

## Canonical layout (on each node, persists on EBS across stop/start)

    /opt/temporalstore/config/temporalstore.toml     # the config file (source of truth)
    /opt/temporalstore/scripts/with_config.sh        # launcher
    /opt/temporalstore/tools/matrixark_load_config.py # loader
    /opt/temporalstore/tools/deploy_config.sh        # applier (this dir)

## How a service reads the config on start

Each unit's `ExecStart` runs THROUGH the launcher:

    ExecStart=/opt/temporalstore/scripts/with_config.sh \
        /opt/temporalstore/config/temporalstore.toml \
        <binary> <args>

`with_config.sh` loads the TOML, exports every mapped env var **only if it is not
already set**, then `exec`s the binary. Precedence is:

    built-in default  <  config file  <  explicit environment variable

Because systemd applies a unit's `Environment=` / `EnvironmentFile=` before
`ExecStart` runs, any explicit env in the unit is already set when the launcher
runs and is left untouched — so **env still wins** over the config file. No
service reader code changes; the existing `os.environ.get` / `std::env::var`
reads pick the values up unchanged.

The canonical unit files are in `deploy/systemd/*.service` (reference / fresh
installs). For an already-provisioned cluster, `deploy_config.sh` wraps the
units in place (see below) without needing to know their exact ExecStart.

## Apply on the NEXT cluster start

The test cluster is normally stopped to save cost (`ts_cluster.sh stop`). On the
next start, run the applier once per node (control, datanode, extractor):

    ts_cluster.sh start                 # bring instances up
    # then, on each node (or via ssh from control):
    sudo /opt/temporalstore/tools/deploy_config.sh --restart

`deploy_config.sh` is idempotent. It:

1. installs the config file + launcher + loader under `/opt/temporalstore`
   (never clobbers an edited `temporalstore.toml` unless `--force-config`);
2. for each installed `matrixark-*` unit, writes an idempotent drop-in
   `/etc/systemd/system/<unit>.service.d/10-with-config.conf` that re-points
   `ExecStart` through the launcher, preserving the unit's current command +
   args (read live from systemd);
3. `daemon-reload` (and `--restart` to restart the wired units now).

After the first apply, the drop-ins persist on EBS, so subsequent starts read
the config automatically; re-run `deploy_config.sh` only to pick up a changed
config file or new units (`--force-config` to replace the deployed TOML).

Requirement: `python3` must be present on every node (the loader is a
stdlib-only script; no third-party TOML library needed).

## Editing the config

Edit `/opt/temporalstore/config/temporalstore.toml` on the node and restart the
service, or edit `config/temporalstore.toml` in the repo and re-run
`deploy_config.sh --force-config --restart`. For a one-off override, set the env
var in the unit (a drop-in `Environment=…`) — it wins over the file.
