FROM python:3.12-alpine
WORKDIR /app
COPY server.py .
ENV PORT=8080
ENV SECRETS_FILE=/app/.env
EXPOSE 8080
CMD ["python", "server.py"]
