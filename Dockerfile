FROM node:20

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci

COPY . .

RUN npm run build


RUN apt-get update \
  && apt-get install -y openssh-server \
  && echo "root:Docker!" | chpasswd

RUN rm -f /etc/ssh/sshd_config
COPY sshd_config /etc/ssh/

COPY start.sh /usr/bin/
RUN chmod +x /usr/bin/start.sh

ENV RUNNING_IN_DOCKER true

EXPOSE 9181 2222

CMD ["bash", "-c", "start.sh && node dist/cjs/src/api/main.js"]
