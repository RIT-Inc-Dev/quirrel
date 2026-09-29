FROM node:20

WORKDIR /usr/src/app

# playwright が Chromium/Firefox 本体をダウンロードして混入するのを防ぐ
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY package*.json ./
RUN npm ci

COPY . .

RUN npm run build

ENV RUNNING_IN_DOCKER true

EXPOSE 9181
CMD node dist/cjs/src/api/main.js
