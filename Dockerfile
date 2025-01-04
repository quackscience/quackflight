FROM node:20-alpine AS build
WORKDIR /app
RUN apk add git
RUN git clone -b selfservice https://github.com/quackscience/quack-ui /app
RUN npm install -g pnpm
RUN npx update-browserslist-db@latest
RUN npm install && npm run build

FROM python:3.8.10-slim
WORKDIR /app
ADD requirements.txt .
RUN apt update && apt install -y binutils wget git \
  && pip install -r requirements.txt \
  && rm -rf /var/lib/apt/lists/* && rm -rf ~/.cache/pip/*
ADD main.py .
COPY --from=build /app/dist ./public
EXPOSE 8123
ARG VITE_SELFSERVICE=true
CMD ["python3","./main.py"]
