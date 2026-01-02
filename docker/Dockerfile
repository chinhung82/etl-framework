FROM mysql:8.0

ENV MYSQL_ROOT_PASSWORD=root
ENV MYSQL_DATABASE=ems
ENV MYSQL_USER=etluser
ENV MYSQL_PASSWORD=etlpass

COPY mysql.sql /docker-entrypoint-initdb.d/
EXPOSE 3306
