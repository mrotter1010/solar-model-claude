# Stage 1: Build frontend
FROM node:20-alpine AS build

WORKDIR /app

# Copy package files first for layer caching
COPY frontend/package.json frontend/package-lock.json ./

RUN npm ci

# Copy frontend source
COPY frontend/ .

# Build-time env vars (baked into the JS bundle by Vite).
# The URL defaults (/api, /orchestrator) are correct for production behind
# Nginx — override only if you need different paths.
# VITE_ANALYSIS_API_KEY must be passed as a build arg for authenticated requests.
ARG VITE_ANALYSIS_API_URL=/api
ARG VITE_ORCHESTRATOR_URL=/orchestrator
ARG VITE_ANALYSIS_API_KEY=
ENV VITE_ANALYSIS_API_URL=$VITE_ANALYSIS_API_URL
ENV VITE_ORCHESTRATOR_URL=$VITE_ORCHESTRATOR_URL
ENV VITE_ANALYSIS_API_KEY=$VITE_ANALYSIS_API_KEY

RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:alpine

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built frontend assets from stage 1
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
