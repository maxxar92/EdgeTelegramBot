docker cp edgenodebot:/home/edgenode/hosts.db .
docker build -t edgenodebot .
docker stop edgenodebot
docker rm edgenodebot
docker run -d --restart unless-stopped --name edgenodebot edgenodebot
docker logs edgenodebot

