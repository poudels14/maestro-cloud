FROM oven/bun:1-alpine
WORKDIR /app
COPY server.js .
RUN bun add dotenv
ENV SECRETS_FILE=/app/.env
EXPOSE 3000
CMD ["bun", "run", "server.js"]
