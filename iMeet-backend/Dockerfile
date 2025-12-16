FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ make default-libmysqlclient-dev pkg-config \
    libgl1 libglib2.0-0 libgomp1 cmake \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies - exclude pycuda (needs CUDA)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir python-dotenv Django mysqlclient gunicorn onnxruntime insightface && \
    grep -v pycuda requirements.txt | pip install --no-cache-dir -r /dev/stdin

COPY . .

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
