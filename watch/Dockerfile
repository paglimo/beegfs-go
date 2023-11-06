# Using distroless static image for enhanced security:
FROM gcr.io/distroless/static:latest
LABEL maintainers="ThinkParQ" \
    description="BeeGFS file system modification event watcher" \
    org.opencontainers.image.description="BeeGFS file system modification event watcher" \
    org.opencontainers.image.source="https://github.com/ThinkParQ/bee-watch" \
    org.opencontainers.image.licenses="BeeGFS EULA"

# Copy the bee-watch binary into the container:
ARG BUILD_DIR=""
COPY ${BUILD_DIR}/bee-watch /bee-watch

# While ideally we'd use a non-root user for better security, we don't know how
# sysFileEventLogTarget will be specified. If the user chooses to bind mount it
# into the container we may need root access to access the Unix socket.
# USER 1000

ENTRYPOINT ["/bee-watch"]