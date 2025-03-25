# DiscoData2API
DiscoData v2 using DataHub infrastructure

## Documentation - API Reference

The public and private API share the same first 2 calls. The others are only available in the private API.

#### Get catalog of views stored in MongoDB - PRIVATE & PUBLIC

```http
  GET /api/View/GetCatalog
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `N/A`     | `N/A`    | **Required**. No parameters required |

#### Execute a view and Fetch data - PRIVATE & PUBLIC

```http
  POST /api/View/Filtered/${id}
```

| Parameter | Type     | Description                                       |
| :-------- | :------- | :------------------------------------------------ |
| `id`      | `string` | **Required**. The Id of mongoDb document to fetch |
| `fields`      | `array[string]` | **Optional**. List of Fields the Select query should fetch |
| `filters`      | `array[string]` | **Optional**. List of filters to fine tune the result of the query  |

#### Execute a view and Fetch data - PRIVATE & PUBLIC

```http
  GET /api/View/${id}
```

| Parameter | Type     | Description                                       |
| :-------- | :------- | :------------------------------------------------ |
| `id`      | `string` | **Required**. The Id of mongoDb document to fetch |
| `fields`      | `array[string]` | **Optional**. List of Fields the Select query should fetch |
| `filters`      | `array[string]` | **Optional**. List of filters to fine tune the result of the query  |

#### Create a view - PRIVATE

```http
  POST /api/createView
```

| Parameter | Type     | Description                                       |
| :-------- | :------- | :------------------------------------------------ |
| `name`      | `string` | **Required**. Name of the query |
| `version`      | `string` | **Required**. i.e : v1, v2.1 Version of the query |
| `query`      | `string` | **Required**. the SQL query |
| `fields`      | `array[name:string,type:string,description:description]` | **Required**. List of fields, array of items with the name, the type and the description of each fields  |

#### Read a view stored in MongoDB - PRIVATE

```http
  GET /api/readView/${id}
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `id`     | `string`    | **Required**. The mongoDb ID of the query document |

#### Update a view - PRIVATE API

```http
  POST /api/updateView/
```
| Parameter | Type     | Description                                       |
| :-------- | :------- | :------------------------------------------------ |
| `id`      | `string` | **Required**. The id of the query |
| `name`      | `string` | **Optional**. Name of the query |
| `version`      | `string` | **Optional**. i.e : v1, v2.1 Version of the query |
| `query`      | `string` | **Optional**. the SQL query |
| `fields`      | `array[name:string,type:string,description:description]` | **Optional**. List of fields, array of items with the name, the type and the description of each fields  |

#### Delete a view - PRIVATE

```http
  POST /api/deleteView/${id}
```

| Parameter | Type     | Description                                       |
| :-------- | :------- | :------------------------------------------------ |
| `id`      | `string` | **Required**. The Id of mongoDb document to delete |

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
