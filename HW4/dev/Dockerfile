FROM python:3.11.8-slim

WORKDIR /app

COPY requirements.txt .
# --no-cache-dir -> do not cache downloads when installing a package
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
RUN rm -rf src
COPY src /app

# Make the scripts in the src directory executable
RUN chmod +x /app/*.py

#ENTRYPOINT also allows you to pass arguments at runtime to customize the container’s behavior.
ENTRYPOINT [ "python" ]
