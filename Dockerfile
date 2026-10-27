# syntax=docker/dockerfile:1

# ビルド専用ツールを本番イメージから除くマルチステージ構成

# ---- build stage --------------------------------------------------------
FROM oven/bun:1.4.4 AS bun

FROM node:24.22.1-bookworm AS build

COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun

WORKDIR /usr/src/app

# playwright が Chromium/Firefox 本体をダウンロードして混入するのを防ぐ
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY . .

RUN bun run build

# ---- production dependencies ---------------------------------------------
FROM node:24.22.1-bookworm-slim AS deps

COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun

WORKDIR /usr/src/app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production --ignore-scripts

# ---- runtime stage -------------------------------------------------------
FROM node:24.22.1-bookworm-slim AS runtime

ENV NODE_ENV=production
ENV RUNNING_IN_DOCKER=true

WORKDIR /usr/src/app

COPY package.json ./
COPY --from=deps /usr/src/app/node_modules ./node_modules

COPY --from=build /usr/src/app/dist ./dist

USER node

EXPOSE 9181
CMD ["node", "dist/cjs/src/api/main.js"]
