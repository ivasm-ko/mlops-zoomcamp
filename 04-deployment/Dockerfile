FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

COPY ["starter.py", "Pipfile", "Pipfile.lock", "./"]

RUN pip install -U pip && pip install pipenv && pipenv install --system --deploy

ENTRYPOINT [ "python", "starter.py", "2023", "05" ]