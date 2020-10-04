# s3ql in Docker

This directory contains scripts added to the [general-purpose Dockerfile](../../Dockerfile).

They aim at making s3ql convenient to safely use in a containerized workflow.

## Quick start

To allow a docker container to mount a filesystem, you need to give it extra permissions.

This is achieved by passing the `/dev/fuse` device, and the `SYS_ADMIN` capacity. This implies a privileged execution context for your container, and you should
thus make sure it is not directly exposed to potentially malicious users.

    docker run
      --device /dev/fuse --cap-add SYS_ADMIN
      -v <host mountpoint>:<guest mountpoint>
      ...
      -e VARIABLE="value"
      ...
      -it
      s3ql/s3ql:stable

Two usage samples with annotated comments can be found in the [examples](examples) directory:

- Using [Docker Compose](examples/docker-compose.yml), see https://docs.docker.com/compose/
- Using [Kubernetes](examples/deployment.yml), see https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

## Configuration

This docker image is configured mostly through environment variables. They are detailed in the following sections.

### General

| Variable             | Required | Docker image default | Documentation
|---	               |---       |---                   |---
| S3QL_STORAGE_URL     | yes      |                      | This is the [storage url](https://www.rath.org/s3ql-docs/backends.html)
| S3QL_MOUNTPOINT      | yes      |                      | This is the [mount point](https://www.rath.org/s3ql-docs/mount.html)
| S3QL_AUTHFILE        | no       |                      | If set, equivalent to `--authfile`
| S3QL_BACKEND_OPTIONS | no       |                      | If set, equivalent to `--backend-options`
| S3QL_CACHEDIR        | no       |                      | If set, equivalent to `--cachedir`
| S3QL_COMPRESS        | no       |                      | If set, equivalent to `--compress`
| S3QL_DEBUG_MODULES   | no       |                      | If set, equivalent to `--debug-modules`
| S3QL_LOG             | no       | `none`               | If set, equivalent to `--log`
| S3QL_QUIET           | no       |                      | If set to **`true`**, add `--quiet`

### `mount.s3ql`

| Variable                            | Docker image default | Documentation
|---                                  |---                   |---
| S3QL_ALLOW_OTHER              | `true`               | If set to **`true`**, add `--allow-other`
| S3QL_ALLOW_ROOT               |                      | If set to **`true`**, add `--allow-root`
| S3QL_CACHESIZE                |                      | If set, equivalent to `--cachesize`
| S3QL_FG                       | `true`               | If set to **`true`**, add `--fg`
| S3QL_FS_NAME                  |                      | If set, equivalent to `--fs-name`
| S3QL_KEEP_CACHE               |                      | If set to **`true`**, add `--keep-cache`
| S3QL_MAX_CACHE_ENTRIES        |                      | If set, equivalent to `--max-cache-entries`
| S3QL_METADATA_UPLOAD_INTERVAL |                      | If set, equivalent to `--metadata-upload-interval`
| S3QL_MIN_OBJ_SIZE             |                      | If set, equivalent to `--min-obj-size` - **not yet implemented**
| S3QL_NFS                      |                      | If set to **`true`**, add `--nfs`
| S3QL_PROFILE                  |                      | If set to **`true`**, add `--profile`
| S3QL_SYSTEMD                  |                      | If set to **`true`**, add `--systemd`
| S3QL_THREADS                  |                      | If set, equivalent to `--threads`

### `fsck.s3ql`

| Variable               | Docker image default | Documentation
|---                     |---                   |---
| S3QL_BATCH        | `true`               | If set to **`true`**, add `--batch`
| S3QL_FORCE        |                      | If set to **`true`**, add `--force`
| S3QL_FORCE_REMOTE |                      | If set to **`true`**, add `--force-remote`
| S3QL_KEEP_CACHE   |                      | If set to **`true`**, add `--keep-cache`

### `umount.s3ql`

| Variable         | Docker image default | Documentation
|---               |---                   |---
| S3QL_LAZY |                      | If set to **`true`**, add `--lazy`
