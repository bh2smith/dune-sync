FROM python:3.12-alpine

RUN mkdir /app
COPY src /app/src
COPY requirements/prod.txt ./requirements-prod.txt
RUN python -m pip install -r ./requirements-prod.txt
USER 1000
WORKDIR /app
ENTRYPOINT [ "python", "-m" , "src.main"]
