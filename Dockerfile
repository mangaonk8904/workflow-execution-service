FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[s3]"
COPY examples ./examples
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
EXPOSE 8000
ENTRYPOINT ["python", "-m", "wes"]
CMD ["api"]
