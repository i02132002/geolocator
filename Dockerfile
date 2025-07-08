FROM python:3.12-slim

WORKDIR /app

RUN apt-get update
RUN apt-get install libpq-dev gcc -y

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy only the pyproject.toml and poetry.lock first
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
RUN poetry install --no-root

# Copy the rest of the application code
COPY app/ .

# Use CMD to run the application with Uvicorn
CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
#CMD ["sleep", "infinity"]
