FROM public.ecr.aws/docker/library/node:16
RUN apt-get update && apt-get install -y aria2 zstd
WORKDIR /app
COPY . ./
RUN npm install && npm install typescript -g
RUN npm run build
CMD aria2c -x 10 -s 10 ${INPUT_URL} -o StateMarketDeals.json.zst && unzstd StateMarketDeals.json.zst && node dist/index.js
