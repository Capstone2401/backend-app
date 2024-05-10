FROM node:20

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install

COPY src/ ./src

RUN npm install typescript ts-node -g

EXPOSE 3000

CMD ["npm", "start"]
