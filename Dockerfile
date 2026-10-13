# syntax=docker/dockerfile:1

# ビルド専用ツールを本番イメージから除くマルチステージ構成

# ---- build stage --------------------------------------------------------
FROM node:24-bookworm AS build

WORKDIR /usr/src/app

# playwright が Chromium/Firefox 本体をダウンロードして混入するのを防ぐ
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY package*.json ./
RUN npm ci

COPY . .

RUN npm run build

# ---- runtime stage -------------------------------------------------------
FROM node:24-bookworm-slim AS runtime

ENV NODE_ENV=production
ENV RUNNING_IN_DOCKER=true

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force

COPY --from=build /usr/src/app/dist ./dist

USER node

EXPOSE 9181
CMD ["node", "dist/cjs/src/api/main.js"]
