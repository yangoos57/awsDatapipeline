FROM python@sha256:02507fe8e3ad7fee64a9104601093b2fe683d6361373f83e69c5bd23abea7b2f

ENV DIR reporting_ec2

RUN apt-get update -y && apt-get install -y gcc

COPY requirements.txt .

RUN pip3 install --upgrade pip

RUN pip3 install -r requirements.txt

COPY container_files ${DIR}/

WORKDIR /${DIR}

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
