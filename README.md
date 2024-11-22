# DiscoData2API
DiscoData v2 using DataHub infrastructure

## Docker Image

### PUBLIC API

Open terminal at BACK folder level

```bash
  docker build -f public/Dockerfile -t disco-data-api .
```
```bash
  docker run -it -p 5205:5205 --name disco-data-api-container disco-data-api
```

### PRIVATE API
Open terminal at back forder level


```bash
  docker build -f private/Dockerfile -t disco-data-api-private .
```
```bash
  docker run -it -p 5243:5243 --name disco-data-api-private-container disco-data-api-private
```