cd jupyterlab-docker
docker build -t jupyterlab .
cd ..
docker run --name jupyterlab -p 8888:8888 -v $(pwd):/app jupyterlab

# docker rm -f jupyterlab 