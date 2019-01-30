FROM postgres:11.1

ENTRYPOINT []
CMD export POSTGRES_DB=discourse; export POSTGRES_USER=postgres; export POSTGRES_PASSWORD=$(cat /etc/secrets/postgres) ;./docker-entrypoint.sh postgres;
