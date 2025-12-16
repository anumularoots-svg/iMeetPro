FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    default-libmysqlclient-dev \
    pkg-config \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies - First install core, then insightface
RUN pip install --upgrade pip && \
    pip install --no-cache-dir Django mysqlclient gunicorn && \
    pip install --no-cache-dir onnxruntime && \
    pip install --no-cache-dir insightface && \
    pip install --no-cache-dir -r requirements.txt; exit 0

# Verify Django installed
RUN python -c "import django; print('Django OK')"

# Copy project
COPY . .

# Expose port
EXPOSE 8000

# Run server
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
