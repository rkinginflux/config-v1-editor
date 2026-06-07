FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY backend/go.mod backend/main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o config-api main.go

FROM nginx:alpine

# Install the Go binary
COPY --from=builder /app/config-api /usr/local/bin/config-api

# Frontend
COPY index.html /usr/share/nginx/html/index.html
RUN chmod 644 /usr/share/nginx/html/index.html

# Nginx config — routes /api to Go, rest to static
RUN printf 'server {\n\
    listen 770;\n\
    root /usr/share/nginx/html;\n\
\n\
    location /api/ {\n\
        proxy_pass http://127.0.0.1:7701;\n\
        proxy_set_header Host $host;\n\
        proxy_set_header X-Real-IP $remote_addr;\n\
    }\n\
\n\
    location / {\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf

# Start both nginx and the Go API
RUN printf '#!/bin/sh\n\
nginx -g "daemon off;" &\n\
/usr/local/bin/config-api\n' > /entrypoint.sh && chmod +x /entrypoint.sh

EXPOSE 770 7701
CMD ["/entrypoint.sh"]
