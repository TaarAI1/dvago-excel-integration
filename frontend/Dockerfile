# Stage 1: Build React app
FROM node:20-slim AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:1.25-alpine

# Install gettext for envsubst
RUN apk add --no-cache gettext

COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx config as a template (BACKEND_URL will be substituted at runtime)
COPY nginx.conf /etc/nginx/conf.d/default.conf.template

EXPOSE 80

# Substitute BACKEND_URL env var into nginx config at container start, then launch nginx
CMD ["/bin/sh", "-c", "envsubst '${BACKEND_URL}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]
