FROM python:3.13.7-slim

#RUN apt-get update && apt-get upgrade -y 
#RUN apt-get install -y curl gnupg2


#RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

#RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && \
    ACCEPT_EULA=Y \
    apt-get install -y \
    #msodbcsql18 \
    libgssapi-krb5-2 \
    build-essential \
    unixodbc-dev

WORKDIR /ccron

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

COPY . /ccron

#RUN mkdir -p /etc/ssl/
#RUN touch /etc/ssl/openssl.cnf
#RUN echo "MinProtocol = TLSv1.0" >> /etc/ssl/openssl.cnf
#RUN echo "CipherString = DEFAULT@SECLEVEL=1" >> /etc/ssl/openssl.cnf

EXPOSE 8000

CMD ["gunicorn", "ccron.src.infrastructure.adapter.web.ccron_web_controller:app", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000"]