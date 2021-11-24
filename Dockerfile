
FROM ubuntu

MAINTAINER Mathew Alexander "mathew.alexander@chubb.com"
LABEL Azure search demo APIs



RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y unar \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update

RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y git


# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN apt-get install -y locales locales-all
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8




WORKDIR /PLA

COPY . .



RUN python3.6 -m pip install -r requirements.txt
RUN [ "python3.6", "-c", "import nltk; nltk.download('stopwords')" ]

RUN [ "python3.6", "-c", "import nltk; nltk.download('punkt')" ]




ENTRYPOINT [ "python3.6" ]

CMD [ "rest.py" ]
