FROM postgres:15.1-alpine3.16

ENV POSTGRES_USER "admin"
ENV POSTGRES_PASSWORD "admin"

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 5432
CMD ["postgres"]
