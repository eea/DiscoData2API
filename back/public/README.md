# DiscoData2API - Data Product API

Public API for querying Dremio DiscoData Virtual Datasets (VDS).

Base path: `/api/data-product`

---

## Endpoints

### 1. Browse Catalog

```
GET /api/data-product
```

Returns the nested folder structure of all available VDS across the gold and silver spaces.

**Response:**
```json
[
  {
    "folder": "gold",
    "folders": [
      {
        "folder": "WISE_SOE",
        "folders": [
          {
            "folder": "latest",
            "vds": [
              {
                "name": "Waterbase_T_WISE6_AggregatedData",
                "path": "discoData.gold.WISE_SOE.latest.Waterbase_T_WISE6_AggregatedData"
              }
            ]
          }
        ]
      }
    ]
  }
]
```

---

### 2. Get VDS Metadata

```
GET /api/data-product/vds/{virtualDatasetName}
```

Returns the schema of a VDS: field names, filterable columns, and the max allowed limit.

**Example:**
```
GET /api/data-product/vds/discoData.gold.WISE_SOE.latest.Waterbase_T_WISE6_AggregatedData
```

**Response:**
```json
{
  "name": "discoData.gold.WISE_SOE.latest.Waterbase_T_WISE6_AggregatedData",
  "fields": ["monitoringSiteIdentifier", "countryCode", "resultObservedValue", ...],
  "filters": ["countryCode", "phenomenonTimeSamplingDate", ...],
  "maxLimit": 10000
}
```

---

### 3. Query a VDS

```
POST /api/data-product/query/{virtualDatasetName}
```

Execute a query against a VDS with optional field selection, filters, aggregates, grouping, and pagination.

**Path parameter:**
- `virtualDatasetName` - Full dot-separated Dremio path (e.g. `discoData.gold.WISE_SOE.latest.Waterbase_T_WISE6_AggregatedData`)

---

## Request Body

```json
{
  "fields": ["col1", "col2"],
  "filters": [...],
  "aggregates": [...],
  "groupBy": ["col1"],
  "limit": 150,
  "offset": 0
}
```

All properties are optional.

| Property     | Type                   | Default | Description                                  |
|-------------|------------------------|---------|----------------------------------------------|
| `fields`    | `string[]`             | `*`     | Columns to select. Omit or null for all.     |
| `filters`   | `FilterDefinition[]`   | none    | WHERE conditions (see below).                |
| `aggregates`| `AggregateDefinition[]`| none    | Aggregate functions (see below).             |
| `groupBy`   | `string[]`             | none    | Columns to group by.                         |
| `limit`     | `int`                  | 150     | Max rows to return (1-10000).                |
| `offset`    | `int`                  | 0       | Number of rows to skip for pagination.       |

---

## Filtering

Filters are pushed into the SQL WHERE clause server-side (Dremio does the filtering, not the API).

Each filter has the following structure:

```json
{
  "fieldName": "countryCode",
  "condition": "=",
  "values": ["FR"],
  "concat": "AND"
}
```

| Property    | Type       | Default | Description                                        |
|------------|------------|---------|----------------------------------------------------|
| `fieldName`| `string`   | required| Column name to filter on.                          |
| `condition`| `string`   | required| SQL operator: `=`, `!=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`, `IS NOT`, `LIKE` |
| `values`   | `string[]` | required| Values for the condition.                          |
| `concat`   | `string`   | `AND`   | Logical concatenation with previous filter (`AND` / `OR`). |

### Filter Examples

**Equality:**
```json
{"fieldName": "countryCode", "condition": "=", "values": ["FR"], "concat": "AND"}
```
Generates: `AND ("countryCode" = 'FR')`

**IN list:**
```json
{"fieldName": "countryCode", "condition": "IN", "values": ["FR", "DE", "ES"], "concat": "AND"}
```
Generates: `AND ("countryCode" IN ('FR', 'DE', 'ES'))`

**BETWEEN (numeric):**
```json
{"fieldName": "lon", "condition": "BETWEEN", "values": ["2.2", "2.5"], "concat": "AND"}
```
Generates: `AND ("lon" BETWEEN 2.2 AND 2.5)`

**IS NOT NULL:**
```json
{"fieldName": "lat", "condition": "IS NOT", "values": ["NULL"], "concat": "AND"}
```
Generates: `AND ("lat" IS NOT NULL)`

**LIKE:**
```json
{"fieldName": "monitoringSiteName", "condition": "LIKE", "values": ["'%Seine%'"], "concat": "AND"}
```
Generates: `AND ("monitoringSiteName" LIKE '%Seine%')`

---

## Aggregates

Aggregate functions are applied server-side and should be combined with `groupBy`.

```json
{
  "function": "COUNT",
  "field": "*",
  "alias": "total"
}
```

| Property      | Type     | Description                                              |
|--------------|----------|----------------------------------------------------------|
| `function`   | `string` | One of: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `DATE_TRUNC` |
| `field`      | `string` | Column name, or `*` for COUNT.                           |
| `alias`      | `string` | Output column name.                                      |
| `granularity`| `string` | Only for `DATE_TRUNC`: `day`, `week`, `month`, `quarter`, `year` |

### Aggregate Examples

**Count per country:**
```json
{
  "fields": ["countryCode"],
  "aggregates": [
    {"function": "COUNT", "field": "*", "alias": "total"}
  ],
  "groupBy": ["countryCode"],
  "limit": 100
}
```
Generates:
```sql
SELECT "countryCode", COUNT(*) AS "total"
FROM ... GROUP BY "countryCode" LIMIT 100
```

**Average value per country:**
```json
{
  "fields": ["countryCode"],
  "aggregates": [
    {"function": "AVG", "field": "resultObservedValue", "alias": "avgValue"},
    {"function": "COUNT", "field": "*", "alias": "total"}
  ],
  "groupBy": ["countryCode"],
  "limit": 100
}
```
Generates:
```sql
SELECT "countryCode", AVG("resultObservedValue") AS "avgValue", COUNT(*) AS "total"
FROM ... GROUP BY "countryCode" LIMIT 100
```

**Monthly time series with DATE_TRUNC:**
```json
{
  "fields": ["countryCode"],
  "aggregates": [
    {"function": "DATE_TRUNC", "field": "phenomenonTimeSamplingDate", "granularity": "month", "alias": "time_period"},
    {"function": "AVG", "field": "resultObservedValue", "alias": "avgValue"}
  ],
  "groupBy": ["countryCode", "time_period"],
  "limit": 1000
}
```
Generates:
```sql
SELECT "countryCode",
       DATE_TRUNC('month', "phenomenonTimeSamplingDate") AS "time_period",
       AVG("resultObservedValue") AS "avgValue"
FROM ... GROUP BY "countryCode", "time_period" LIMIT 1000
```

---

## Pagination

Use `limit` and `offset` to paginate through results.

```json
{
  "limit": 100,
  "offset": 0
}
```

- First page: `offset: 0`, `limit: 100`
- Second page: `offset: 100`, `limit: 100`
- Third page: `offset: 200`, `limit: 100`

---

## Full Example

Query spatial monitoring sites in France within a bounding box, paginated:

```
POST /api/data-product/query/discoData.gold.WISE_SOE.latest.Waterbase_S_WISE_SpatialObject_DerivedData
```

```json
{
  "fields": [
    "thematicIdIdentifier",
    "thematicIdIdentifierScheme",
    "monitoringSiteIdentifier",
    "monitoringSiteName",
    "countryCode",
    "lat",
    "lon"
  ],
  "filters": [
    {"fieldName": "lat", "condition": "IS NOT", "values": ["NULL"], "concat": "AND"},
    {"fieldName": "lon", "condition": "IS NOT", "values": ["NULL"], "concat": "AND"},
    {"fieldName": "countryCode", "condition": "=", "values": ["FR"], "concat": "AND"},
    {"fieldName": "lon", "condition": "BETWEEN", "values": ["2.2", "2.5"], "concat": "AND"},
    {"fieldName": "lat", "condition": "BETWEEN", "values": ["48.8", "48.9"], "concat": "AND"}
  ],
  "limit": 1000,
  "offset": 0
}
```

Generated SQL:
```sql
SELECT "thematicIdIdentifier", "thematicIdIdentifierScheme",
       "monitoringSiteIdentifier", "monitoringSiteName", "countryCode",
       "lat", "lon"
FROM "discoData"."gold"."WISE_SOE"."latest"."Waterbase_S_WISE_SpatialObject_DerivedData"
WHERE 1=1
  AND ("lat" IS NOT NULL)
  AND ("lon" IS NOT NULL)
  AND ("countryCode" = 'FR')
  AND ("lon" BETWEEN 2.2 AND 2.5)
  AND ("lat" BETWEEN 48.8 AND 48.9)
LIMIT 1000 OFFSET 0
```

---

## Error Codes

| Code | Meaning                                        |
|------|------------------------------------------------|
| 200  | Success                                        |
| 400  | Invalid request, bad VDS path, or unsafe SQL   |
| 404  | VDS not found or no data                       |
| 408  | Request timed out                              |

---

## Security

All queries are validated against SQL injection patterns before execution. The following are blocked:
- SQL modification keywords (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`)
- Comment syntax (`--`, `/* */`)
- Semicolons
- Non-ASCII characters
- Aggregate functions are whitelisted: only `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `DATE_TRUNC` are allowed
