
FROM astrocrpublic.azurecr.io/runtime:3.1-5

RUN pip install openelectricity
RUN pip install --upgrade "sqlalchemy>=2.0"