# syntax=docker/dockerfile:1
FROM node:20-alpine AS base

ENV NODE_ENV=production \
    PORT=3333

WORKDIR /app

# Only copy the epg-viewer app to keep context small
COPY epg-viewer/package*.json ./epg-viewer/

WORKDIR /app/epg-viewer
RUN npm ci --omit=dev

# Copy app sources
COPY epg-viewer/. .

# Create data dir (will be mounted as a volume in compose)
RUN mkdir -p /app/epg-viewer/data

EXPOSE 3333

CMD ["npm", "start"]

