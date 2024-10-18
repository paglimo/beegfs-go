# Using distroless static image for enhanced security:
FROM gcr.io/distroless/static:latest

# Copy the bee-watch binary into the container:
COPY beegfs-watch /beegfs-watch

# While ideally we'd use a non-root user for better security, we don't know how
# sysFileEventLogTarget will be specified. If the user chooses to bind mount it
# into the container we may need root access to access the Unix socket.
# USER 1000

ENTRYPOINT ["/beegfs-watch"]