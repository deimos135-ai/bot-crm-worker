FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# web-процес за замовчуванням
CMD ["uvicorn", "app_web.main:app", "--host", "0.0.0.0", "--port", "8080"]
