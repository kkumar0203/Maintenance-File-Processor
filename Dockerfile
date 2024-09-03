FROM python:3.12.4

COPY src /urm/src/
COPY db /urm/db/
COPY files /urm/files/

RUN pip install -r /urm/src/requirements.txt

WORKDIR /urm/src

CMD ["python", "Run.py"]

