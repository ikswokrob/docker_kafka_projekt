FROM python:3.11
WORKDIR /app
COPY dane.csv load_data.py ./
RUN pip install pandas sqlalchemy psycopg2
CMD ["python", "load_data.py"]