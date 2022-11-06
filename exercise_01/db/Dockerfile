FROM postgres:9.6.24-alpine

ENV PGDATA "/data/pgdata"
ENV POSTGRES_USER "admin"
ENV POSTGRES_PASSWORD "admin"

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 5432
CMD ["postgres", "-c", "shared_buffers=128kB"]
