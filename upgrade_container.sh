docker cp edgenodebot:/home/edgenode/hosts.db .
docker cp edgenodebot:/home/edgenode/registeredClients.db .
docker cp edgenodebot:/home/edgenode/hosts.db .
docker cp edgenodebot:/home/edgenode/dadi_historical_prices.db .
docker cp edgenodebot:/home/edgenode/testdata/historical_prices_dadi.json testdata/historical_prices_dadi.json
docker cp edgenodebot:/home/edgenode/testdata/payouts.json testdata/payouts.json
docker build -t edgenodebot .
docker stop edgenodebot
docker rm edgenodebot
docker run -d --restart unless-stopped -p 0.0.0.0:5000:5000 --name edgenodebot edgenodebot
docker logs edgenodebot

