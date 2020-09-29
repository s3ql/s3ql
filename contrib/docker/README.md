# s3ql in Docker

This directory contains scripts added to the [general-purpose Dockerfile](../../Dockerfile).

They aim at making s3ql convenient to safely use in a containerized workflow.

## Quick start

To allow a docker container to mount a filesystem, you need to give it extra permissions.

This is achieved by passing the `/dev/fuse` device, and the `SYS_ADMIN` capacity. This implies a
privileged execution context for your container, and you should thus make sure it is not directly
exposed to potentially malicious users.

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

This docker image is configured mostly through environment variables. They are detailed in the following section.

### Required environment variables

These variables **must** be set for operation.

| Variable    | Example                             | Documentation
|---	      |---                                  |---
| STORAGE_URL | `s3c://my-cloud-provider/my-bucket` | This is the [storage url](https://www.rath.org/s3ql-docs/backends.html) passed to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others)
| MOUNT_POINT | `/mount/data`                       | This is the [mount point](https://www.rath.org/s3ql-docs/mount.html) passed to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others). It will be created if it does not exist yet.

### Optional environment variables

These variables **can** be set, but are not strictly needed. Note that you should still consider looking into each of them and understand their meaning.

| Variable                 | Documentation
|---                       |---
| AUTH_FILE                | If set, add it as value of `--authfile`, see [authentication file](https://www.rath.org/s3ql-docs/authinfo.html)
| LOG_FILE                 | If set, add it as value of `--log`, see [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others). **The `syslog` and `none` options are not well supported**
| CACHE_DIR                | If set, add it as value of `--cachedir`, see [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others)
| CACHE_SIZE               | If set, add it as value of `--cachesize`, see [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| MAX_CACHE_ENTRIES        | If set, add it as value of `--max-cache-entries`, see [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| COMPRESS                 | If set, add it as value of `--compress`, see [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others)
| BACKEND_OPTIONS          | If set, add it as value of `--backend-options` to [storage backend options](https://www.rath.org/s3ql-docs/backends.html)
| FS_NAME                  | If set, add it as value of `--fs-name` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| METADATA_UPLOAD_INTERVAL | If set, add it as value of `--metadata-upload-interval` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| UPLOAD_THREADS           | If set, add it as value of `--threads` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| KEEP_CACHE               | If set to **`true`**, add `--keep-cache` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others)
| ALLOW_OTHER              | If set to **`true`**, add `--allow-other` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| ALLOW_ROOT               | If set to **`true`**, add `--allow-root` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| NFS                      | If set to **`true`**, add `--nfs` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html)
| UMOUNT_LAZY              | If set to **`true`**, add `--lazy` to [umount.s3ql](https://www.rath.org/s3ql-docs/umount.html). **It is not added to `fusermount`, which is invoked on failure of `umount.s3ql`**
| FSCK_FORCE               | If set to **`true`**, add `--force` to [fsck.s3ql](https://www.rath.org/s3ql-docs/fsck.html#checking-and-repairing-internal-file-system-errors)
| VERBOSE                  | If set to **`true`**, add `--debug` to [mount.s3ql](https://www.rath.org/s3ql-docs/mount.html) (and others), and log image configuration parsing messages
