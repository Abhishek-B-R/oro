FROM nginx:alpine
RUN apk add --no-cache openssl
COPY tests/integration/inference-mock.conf /etc/nginx/conf.d/default.conf
CMD ["/bin/sh", "-c", "openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/mock.key -out /tmp/mock.crt -days 1 -subj /CN=inference-mock >/dev/null 2>&1 && exec nginx -g 'daemon off;'"]
