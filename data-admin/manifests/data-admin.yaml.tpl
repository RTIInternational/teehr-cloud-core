# Admin access to the shared /data drive.
#
# Every pod that mounts /data -- JupyterHub singleuser, Spark executors,
# Prefect jobs -- runs as uid 1000, and JupyterHub "admin" is a hub-level
# role with no POSIX privilege behind it. So when a directory ends up owned
# by another uid, nobody can clear it: removing a file needs write+execute on
# its PARENT directory, not on the file.
#
# Two such trees exist from before the current setup standardised on uid 1000
# (see RTIInternational/teehr-cloud-core#69):
#   /data/spark-temp   drwxr-xr-x root:root  -- not group-writable
#   /data/mgd_temp     drwxrwxr-x 185:185    -- 185 is the upstream Spark image uid
#
# This pod runs as root so an admin can clear or re-own anything. The EFS
# volume is a plain NFS mount with no root squash, so root here is root on the
# filesystem. That is a standing privileged foothold, but anyone who can
# `kubectl exec` into this namespace can already schedule an equivalent pod,
# so it does not widen the blast radius meaningfully.
#
#   kubectl exec -it -n ${environment.namespace} deploy/data-admin -- bash
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-admin
  namespace: ${environment.namespace}
  labels:
    app: data-admin
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: data-admin
  template:
    metadata:
      labels:
        app: data-admin
    spec:
      securityContext:
        runAsUser: 0
        runAsGroup: 0
      initContainers:
        # Idempotent, and deliberately bounded to depth 1. A recursive pass
        # over /data is not viable: it holds ~3M files across ~938 GiB and a
        # single metadata walk took over two hours. Setting the setgid bit on
        # the top level is enough for everything created from here on, because
        # a new directory inherits both the gid AND the setgid bit from its
        # parent. Repairing existing deep trees is a manual admin task.
        #
        # Note this does not make new FILES group-writable -- that depends on
        # each writer's umask (default 022), which is why directories such as
        # /data/temp-spark are drwxr-xr-x. That is harmless while every writer
        # runs as uid 1000 and is therefore the owner; the group bits are
        # belt-and-braces for the cross-uid case.
        - name: ensure-base-permissions
          image: debian:stable-slim
          command:
            - /bin/sh
            - -c
            - |
              set -u
              echo "before:"; ls -land /data
              chgrp 1000 /data 2>/dev/null || true
              chmod 2775 /data 2>/dev/null || true
              find /data -maxdepth 1 -mindepth 1 -type d -exec chgrp 1000 {} + 2>/dev/null || true
              find /data -maxdepth 1 -mindepth 1 -type d -exec chmod g+rwXs {} + 2>/dev/null || true
              echo "after:"; ls -land /data
          volumeMounts:
            - name: teehr-hub-data-nfs
              mountPath: /data
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
            limits:
              cpu: 500m
              memory: 256Mi
      containers:
        - name: data-admin
          image: debian:stable-slim
          command: ["/bin/sh", "-c", "sleep infinity"]
          volumeMounts:
            - name: teehr-hub-data-nfs
              mountPath: /data
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
            limits:
              cpu: 500m
              memory: 512Mi
      volumes:
        - name: teehr-hub-data-nfs
          persistentVolumeClaim:
            claimName: data-nfs
