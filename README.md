# DiscoData2API
DiscoData v2 using DataHub infrastructure

## Docker Image

### PUBLIC API

Open terminal at BACK folder level

It creates an image for each environment (Check Configs folder)

```bash
  docker build  --build-arg ENVIRONMENT={ENVIRONMENT}   -f public/Dockerfile -t txalaparta/disco-data-api_(ENVIRONMENT) .
```
```bash
  docker run -it -p 5205:5205 --name disco-data-api-container txalaparta/disco-data-api_(ENVIRONMENT)
```

### PRIVATE API
Open terminal at back forder level.
It creates an image for each environment (Check Configs folder)


```bash
  docker build  --build-arg ENVIRONMENT={ENVIRONMENT} -f private/Dockerfile -t txalaparta/disco-data-api-private_(ENVIRONMENT) .
```
```bash
  docker run -it -p 5243:5243 --name disco-data-api-private-container txalaparta/disco-data-api-private_(ENVIRONMENT)
```

### PRIVATE API
Open terminal at back forder level


```bash
  docker build -f private/Dockerfile -t disco-data-api-private .
```
```bash
  docker run -it -p 5243:5243 --name disco-data-api-private-container disco-data-api-private
```

