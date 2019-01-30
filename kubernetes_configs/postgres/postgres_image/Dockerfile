FROM postgres:9.4

ENTRYPOINT []
CMD export POSTGRES_DB=guestbook; export POSTGRES_USER=django_user; export POSTGRES_PASSWORD=$(cat /etc/secrets/djangouserpw) ;./docker-entrypoint.sh postgres;
