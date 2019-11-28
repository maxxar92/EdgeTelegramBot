docker cp edgenodebot:/home/edgenode/hosts.db .
docker build -t edgenodebot .
docker stop edgenodebot
docker rm edgenodebot
docker run -d --restart unless-stopped -p 0.0.0.0:5000:5000 --name edgenodebot edgenodebot
docker logs edgenodebot

