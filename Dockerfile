FROM python:3.11.4

# Set the working directory in the container
WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD ["python","-u","controller.py"]