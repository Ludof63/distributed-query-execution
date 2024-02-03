FROM ubuntu as builder
WORKDIR /src
RUN apt update && apt install -y cmake g++ libcurl4-openssl-dev

COPY . .
RUN mkdir build cmake-build-debug
RUN cd cmake-build-debug && cmake .. && make


FROM builder as coordinator
CMD ./cmake-build-debug/coordinator https://db.in.tum.de/teaching/ws2324/clouddataprocessing/data/filelist.csv 4242

FROM builder as worker
CMD ./cmake-build-debug/worker ${COORDINATOR} 4242