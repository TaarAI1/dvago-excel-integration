# Stage 1: Build React app
FROM node:20-slim AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:1.25-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx config as a template (__BACKEND_URL__ replaced at runtime by sed)
COPY nginx.conf /etc/nginx/conf.d/default.conf.template

EXPOSE 80

# Replace placeholders with env vars, then start nginx
CMD ["/bin/sh", "-c", "sed \"s|__BACKEND_URL__|$BACKEND_URL|g;s|__PORT__|${PORT:-80}|g\" /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]
