FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6

# RUN python -m venv venv && source venv/bin/activate
RUN git clone https://github.com/kostyamyasso2002/SoCPC-hackaton.git
WORKDIR SoCPC-hackaton
RUN git checkout dev && pip install -r requirements.txt
WORKDIR ..
RUN wget https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
RUN tar -xzf kafka_2.13-3.0.0.tgz
RUN touch data/out_cm.json

CMD ["python3", "SoCPC-hackaton/src/start.py"]
