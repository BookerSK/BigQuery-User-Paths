# GA4 User Path Analysis with MySQL, Python, and BigQuery

Learn how to practice Google Analytics 4 (GA4) analysis in MySQL by building user paths and tracking source/medium. Step-by-step guide with SQL code, dynamic queries, and session analysis using GA4 event data.

## Idea

We have a BigQuery table imported from GA4. The full scheme explanation is available [here](https://www.pvalues-blog.com/article/28), but we are interested in `event_timestamp`, `user_pseudo_id`, and `event_params`, because we're interested in User Paths only. Our final goal is to make a table of `user_pseudo_ids` and source/medium steps.

### GA4 Raw Event Data

| Event Timestamp | User Pseudo ID | Event Params (Key / Int Value / String Value) |
|-----------------|----------------|-----------------------------------------------|
| 1695984000000 | user_123 | • key: ga_session_id, int_value: 000000001, string_value: null<br>• key: source, int_value: null, string_value: google<br>• key: medium, int_value: null, string_value: cpc |
| 1695984060000 | user_123 | • key: ga_session_id, int_value: 000000002, string_value: null<br>• key: source, int_value: null, string_value: meta<br>• key: medium, int_value: null, string_value: cpc |
| 1695984120000 | user_456 | • key: ga_session_id, int_value: 000000003, string_value: null<br>• key: source, int_value: null, string_value: google<br>• key: medium, int_value: null, string_value: organic |

### Transformed User Steps

| User Pseudo ID | Step 1 | Step 2 |
|----------------|--------|--------|
| user_123 | google/cpc | meta/cpc |
| user_456 | google/organic | - |

## Why MySQL?

The first thing you face when working with BigQuery - it's their costs. It's not applicable if you have small datasets, but what's the point of data analysis practice if insights are available in a simple CSV file?

Anyway, my practice with BigQuery was always nervous. For example, I paid some dollars working with the [Google Transparency Center dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-data/google-ads-transparency-center), because even:

```sql
SELECT COUNT(*) FROM 'bigquery-public-data.google_ads_transparency_center.creative_stats'
```

costed 150 GB per call. With 1 TB of free usage, you can imagine it will take 7 calls to start paying, and if you have experience with SQL you need many calls to make sure everything works.

So finally, I don't want to pay extra for practice.

Despite this article primarily using MySQL syntax, I have also translated it into Python and BigQuery. The Python representation is done with Pandas, and if you are familiar with its terminology, it provides a better representation than SQL, which is table-oriented and requires considerable practice to understand the logic behind the queries.

## Download the Dataset

First, you need to copy [this dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=ga4_obfuscated_sample_ecommerce&t=events_20210131&page=table) to your BigQuery project so you can upload it to your PC. In the next article, I will generate this dataset synthetically, because the [BigQuery sample dataset for Google Analytics ecommerce web implementation](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset) has some natural data, but it is not applicable for User Paths analysis (they have only one cpc source/medium: google/cpc).

```sql
CREATE TABLE `my_project.my_dataset.my_table` AS 
SELECT * FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_202*`
```

- `my_project` → your project ID
- `my_dataset` → your dataset ID
- `my_table` → the new table you are creating

The `events_202*` pattern selects all GA4 events tables for 2021 (partitioned by date).

BigQuery tables are referenced as: `project_id.dataset_id.table_id`

- **Project ID** → Your Google Cloud project name. You can see it in the BigQuery console at the top left.
- **Dataset ID** → The dataset (folder) inside your project. It groups related tables.
- **Table ID** → The specific table you want to query or create.

Next, you need to use Python to upload data from BigQuery to MySQL:

```python
import pandas_gbq
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from google.oauth2 import service_account

# --- MySQL connection via SQLAlchemy ---
# TODO fill your local MySQL user and password
engine = create_engine(
    "mysql+pymysql://user:password@localhost/mydb?charset=utf8mb4"
)

# --- BigQuery base SQL ---
base_sql = """ 
SELECT
  event_date,
  event_timestamp,
  event_name,
  TO_JSON_STRING(event_params) AS event_params,
  event_previous_timestamp,
  IFNULL(event_value_in_usd, 0) AS event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,
  user_id,
  user_pseudo_id,
  TO_JSON_STRING(privacy_info) AS privacy_info,
  TO_JSON_STRING(user_properties) AS user_properties,
  user_first_touch_timestamp,
  TO_JSON_STRING(user_ltv) AS user_ltv,
  TO_JSON_STRING(device) AS device,
  TO_JSON_STRING(geo) AS geo,
  TO_JSON_STRING(app_info) AS app_info,
  TO_JSON_STRING(traffic_source) AS traffic_source,
  stream_id,
  platform,
  TO_JSON_STRING(event_dimensions) AS event_dimensions,
  TO_JSON_STRING(ecommerce) AS ecommerce,
  TO_JSON_STRING(items) AS items
FROM `my_project.my_dataset.my_table`
"""

credentials = service_account.Credentials.from_service_account_file('secret.json')
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = "my_project"

total_rows = 4295584
chunk_size = 100000

for offset in range(100000, total_rows, chunk_size):
    sql = f"{base_sql} LIMIT {chunk_size} OFFSET {offset}"
    print(f"Fetching rows {offset} to {offset + chunk_size}...")

    df = pandas_gbq.read_gbq(sql)

    if df.empty:
        print("No more rows to fetch.")
        break

    # Fill numeric NaN with 0
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].fillna(0)

    # Replace text NaN with None
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].where(pd.notnull(df[col]), None)

    # --- Push chunk to MySQL ---
    df.to_sql(
        'ga4_ecom',
        con=engine,
        if_exists='append',
        index=False
    )

    print(f"Inserted chunk ending at row {offset + len(df)}")

print("All data inserted successfully.")
```

Note: I processed 43 queries (chunks) to upload 4,295,584 rows from the sample dataset.

You need to have a Google Cloud project with BigQuery API enabled and create a secret from a service account. You can read about credentials more [here](https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.service_account.html), and it looks complex, but shortly, you need to go to [IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts), create a service account, create a secret key, and grant access with the [IAM page](https://console.cloud.google.com/iam-admin/iam).

## MySQL Queries

In this section, I explain the step-by-step process of working with GA4 data with the final Procedure to create all tables in one Call.

The first table we need to create is an unnested table of JSON values in `event_params`. We're interested in `ga_session_id`, `source`, `medium`.

### Step 1: Create `jjson_table` with Unnested Params

#### MySQL (JSON_TABLE)

```sql
CREATE TABLE jjson_table_sample AS
SELECT
  t.event_timestamp,
  t.user_pseudo_id,
  tt.kkey,
  tt.int_value,
  tt.string_value
FROM test_sample AS t
JOIN JSON_TABLE(
  t.event_params,
  "$[*]" COLUMNS (
    kkey VARCHAR(100) PATH "$.key",
    int_value BIGINT PATH "$.value.int_value",
    string_value VARCHAR(255) PATH "$.value.string_value"
  )
) AS tt
WHERE tt.kkey IN ('ga_session_id','source','medium');
```

#### Python (pandas)

```python
import pandas as pd
import json

# Raw data as a list of dictionaries
data = [
    {
        "event_timestamp": 1695984000000,
        "user_pseudo_id": "user_123",
        "event_params": [
            {"key": "ga_session_id", "int_value": 1, "string_value": None},
            {"key": "source", "int_value": None, "string_value": "google"},
            {"key": "medium", "int_value": None, "string_value": "cpc"}
        ]
    },
    {
        "event_timestamp": 1695984060000,
        "user_pseudo_id": "user_123",
        "event_params": [
            {"key": "ga_session_id", "int_value": 2, "string_value": None},
            {"key": "source", "int_value": None, "string_value": "meta"},
            {"key": "medium", "int_value": None, "string_value": "cpc"}
        ]
    },
    {
        "event_timestamp": 1695984120000,
        "user_pseudo_id": "user_456",
        "event_params": [
            {"key": "ga_session_id", "int_value": 3, "string_value": None},
            {"key": "source", "int_value": None, "string_value": "google"},
            {"key": "medium", "int_value": None, "string_value": "organic"}
        ]
    }
]

# Flatten event_params
rows = []
for row in data:
    flat_row = {
        "event_timestamp": row["event_timestamp"],
        "user_pseudo_id": row["user_pseudo_id"]
    }
    for param in row["event_params"]:
        value = param["int_value"] if param["int_value"] is not None else param["string_value"]
        flat_row[param["key"]] = value
    rows.append(flat_row)

# Create DataFrame
jjson_table_sample = pd.DataFrame(rows)
jjson_table_sample = jjson_table_sample[
    jjson_table_sample["kkey"].isin(["ga_session_id","source","medium"])
]
```

#### BigQuery (UNNEST)

```sql
CREATE TABLE dataset.jjson_table_sample AS
SELECT
  event_timestamp,
  user_pseudo_id,
  JSON_VALUE(param, '$.key') AS kkey,
  SAFE_CAST(JSON_VALUE(param, '$.value.int_value') AS INT64) AS int_value,
  JSON_VALUE(param, '$.value.string_value') AS string_value
FROM `project.dataset.test_sample` AS t,
UNNEST(JSON_EXTRACT_ARRAY(t.event_params)) AS param
WHERE JSON_VALUE(param, '$.key') IN ('ga_session_id','source','medium');
```

### Step 2: Apply Session ID to All Events

Note: GA4 table contains many events like page_view, scroll, begin_checkout, purchase, and we don't need all of them. So we apply session_id to every event_timestamp, user_pseudo_id by taking their MAX value of int_value by Partitions.

#### MySQL/BigQuery

```sql
SELECT 
    event_timestamp, 
    user_pseudo_id, 
    kkey, 
    int_value, 
    string_value,
    MAX(int_value) OVER(PARTITION BY event_timestamp, user_pseudo_id) AS session_id
FROM jjson_table;
```

#### Python

```python
import pandas as pd

df = pd.DataFrame(data)

# Compute max_session_id per (event_timestamp, user_pseudo_id)
df["session_id"] = df.groupby(
    ["event_timestamp", "user_pseudo_id"]
)["int_value"].transform("max")

print(df)
```

### Step 3: Create Session Start Table

Now let's form a table with session_start, user_pseudo_id, session_id by taking the minimal event_timestamp grouped by user_pseudo_id and session_id.

#### MySQL/BigQuery

```sql
CREATE TABLE time_user_session AS
WITH sess AS 
(SELECT 
    event_timestamp, 
    user_pseudo_id, 
    kkey, 
    int_value, 
    string_value,
    MAX(int_value) OVER(PARTITION BY event_timestamp, user_pseudo_id) AS session_id
FROM jjson_table)
SELECT 
    MIN(event_timestamp) AS session_start, 
    user_pseudo_id, 
    session_id
FROM sess
GROUP BY 
    user_pseudo_id, 
    session_id;
```

#### Python

```python
import pandas as pd

df = pd.DataFrame(data)

# Step 1: Compute session_id as max(int_value) per (event_timestamp, user_pseudo_id)
df['session_id'] = df.groupby(['event_timestamp', 'user_pseudo_id'])['int_value'].transform('max')

# Step 2: Aggregate to get session_start per (user_pseudo_id, session_id)
time_user_session = df.groupby(['user_pseudo_id', 'session_id'], as_index=False).agg(
    session_start=('event_timestamp', 'min')
)

print(time_user_session)
```

### Step 4: Concatenate Source/Medium

Next step, we need to concatenate our source/medium into one column. To do it, we use a Left join to the just-created time_user_session table (so we filter non-session start rows to prevent duplicates). Also, we need to fill empty sources or mediums with 'not defined'.

#### MySQL/BigQuery

```sql
CREATE TABLE start_user_session_source AS
SELECT 
    t.session_start,
    t.user_pseudo_id,
    t.session_id,
    CONCAT_WS(
        '/',
        COALESCE(MAX(CASE WHEN rt.kkey = 'source' THEN rt.string_value END), 'not defined'),
        COALESCE(MAX(CASE WHEN rt.kkey = 'medium' THEN rt.string_value END), 'not defined')
    ) AS source_medium
FROM time_user_session t
LEFT JOIN jjson_table rt
       ON t.session_start = rt.event_timestamp
      AND t.user_pseudo_id = rt.user_pseudo_id
GROUP BY t.session_start, t.user_pseudo_id, t.session_id;
```

#### Python

```python
import pandas as pd

# Merge time_user_session with jjson_table
merged = time_user_session.merge(
    jjson_df,
    left_on=['session_start', 'user_pseudo_id'],
    right_on=['event_timestamp', 'user_pseudo_id'],
    how='left'
)

# Pivot source/medium
pivoted = merged.pivot_table(
    index=['session_start', 'user_pseudo_id', 'session_id'],
    columns='kkey',
    values='string_value',
    aggfunc='first'
).reset_index()

# Fill missing values
pivoted['source'] = pivoted.get('source', pd.Series()).fillna('not defined')
pivoted['medium'] = pivoted.get('medium', pd.Series()).fillna('not defined')

pivoted['source_medium'] = pivoted['source'] + '/' + pivoted['medium']

start_user_session_source = pivoted[['session_start', 'user_pseudo_id', 'session_id', 'source_medium']]
```

### Step 5: Dynamic User Path Pivot

The next step is as brilliant as simple. If you know Window functions, there is the ROW_NUMBER function, which applies row numbers to every partition. So we need to sort our table by session_start and form columns for 1st row number, for 2nd row number, for 3rd and so on.

Basic concept:

```sql
SELECT
  user_id,
  MAX(CASE WHEN rn = 1 THEN source END) AS step1,
  MAX(CASE WHEN rn = 2 THEN source END) AS step2,
  MAX(CASE WHEN rn = 3 THEN source END) AS step3
  -- Continue if you need more steps
FROM (
  SELECT
    user_id,
    source,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY start_session) AS rn
  FROM sessions
) AS ordered
GROUP BY user_id;
```

But we want a dynamic procedure that can be called in one string.

#### MySQL Stored Procedure

```sql
DELIMITER $$

CREATE PROCEDURE build_user_path()
BEGIN
    -- Declare constants for loops
    DECLARE max_steps INT DEFAULT 0;
    DECLARE i INT DEFAULT 1;
    
    DROP TABLE IF EXISTS jjson_table,time_user_session,start_user_session_source;

    CREATE TABLE jjson_table AS
    SELECT event_timestamp, user_pseudo_id, kkey, int_value, string_value FROM 
        ga4_ecom, 
        JSON_TABLE(
            event_params,
            "$[*]" COLUMNS
                (kkey VARCHAR(100) PATH "$.key",
                int_value BIGINT PATH "$.value.int_value",
                string_value VARCHAR(100) PATH "$.value.string_value")
        ) AS tt
    WHERE tt.kkey IN ('ga_session_id','source','medium');

    CREATE TABLE time_user_session AS
    WITH sess AS 
    (SELECT 
        event_timestamp, 
        user_pseudo_id, 
        kkey, 
        int_value, 
        string_value,
        MAX(int_value) OVER(PARTITION BY event_timestamp, user_pseudo_id) AS session_id
    FROM jjson_table)
    SELECT 
        MIN(event_timestamp) AS session_start, 
        user_pseudo_id, 
        session_id
    FROM sess
    GROUP BY 
        user_pseudo_id, 
        session_id;

    CREATE TABLE start_user_session_source AS
    SELECT 
        t.session_start,
        t.user_pseudo_id,
        t.session_id,
        CONCAT_WS(
            '/',
            COALESCE(MAX(CASE WHEN rt.kkey = 'source' THEN rt.string_value END), 'not defined'),
            COALESCE(MAX(CASE WHEN rt.kkey = 'medium' THEN rt.string_value END), 'not defined')
        ) AS source_medium
    FROM time_user_session t
    LEFT JOIN jjson_table rt
           ON t.session_start = rt.event_timestamp
          AND t.user_pseudo_id = rt.user_pseudo_id
    GROUP BY t.session_start, t.user_pseudo_id, t.session_id;

    -- Maximum number of steps per user
    SELECT MAX(cnt) INTO max_steps
    FROM (
        SELECT user_pseudo_id, COUNT(*) AS cnt
        FROM start_user_session_source
        GROUP BY user_pseudo_id
    ) t;

    -- Build CASE expressions
    SET @case_expressions = '';
    SET i = 1;
    WHILE i <= max_steps DO
        IF i > 1 THEN
            SET @case_expressions = CONCAT(@case_expressions, ', ');
        END IF;
        SET @case_expressions = CONCAT(
            @case_expressions,
            'MAX(CASE WHEN rn = ', i, ' THEN source_medium END) AS step', i
        );
        SET i = i + 1;
    END WHILE;

    -- Build the final SQL
    SET @sql_text = CONCAT(
        'SELECT user_pseudo_id, ', @case_expressions,
        ' FROM (',
          'SELECT user_pseudo_id, source_medium, ',
          'ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY session_start) AS rn ',
          'FROM start_user_session_source',
        ') AS ordered ',
        'GROUP BY user_pseudo_id'
    );

    -- Drop existing target
    DROP TABLE IF EXISTS user_paths;

    -- Create table from dynamic SQL
    SET @create_sql = CONCAT('CREATE TABLE user_paths AS ', @sql_text);
    PREPARE stmt FROM @create_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
END $$

DELIMITER ;
```

#### Python Implementation

```python
import pandas as pd
import json

# Build jjson_table, time_user_session, start_user_session_source
# (using code from previous steps)

# Compute number of steps per user
user_steps = start_user_session_source.groupby('user_pseudo_id').size().reset_index(name='cnt')
max_steps = user_steps['cnt'].max()

# Assign row numbers per user
start_user_session_source['rn'] = start_user_session_source.groupby('user_pseudo_id')['session_start'].rank(method='first').astype(int)

# Build wide table
user_paths = start_user_session_source.pivot(
    index='user_pseudo_id',
    columns='rn',
    values='source_medium'
).fillna('not defined')

# Rename columns to step1, step2, ...
user_paths.columns = [f'step{i}' for i in user_paths.columns]
user_paths = user_paths.reset_index()

print(user_paths)
```

#### BigQuery Procedure

```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.build_user_path`()
BEGIN
  DECLARE max_steps INT64;
  DECLARE i INT64 DEFAULT 1;
  DECLARE case_expressions STRING DEFAULT '';
  DECLARE sql_text STRING;
  
  -- Extract event params
  CREATE OR REPLACE TABLE `my_project.my_dataset.jjson_table` AS
  SELECT
    event_timestamp,
    user_pseudo_id,
    param.key AS kkey,
    param.value.int_value AS int_value,
    param.value.string_value AS string_value
  FROM
    `my_project.my_dataset.my_table`,
    UNNEST(event_params) AS param
  WHERE
    param.key IN ('ga_session_id', 'source', 'medium');

  -- Aggregate sessions
  CREATE OR REPLACE TABLE `my_project.my_dataset.time_user_session` AS
  WITH sess AS (
    SELECT
      event_timestamp,
      user_pseudo_id,
      kkey,
      int_value,
      string_value,
      MAX(int_value) OVER(PARTITION BY event_timestamp, user_pseudo_id) AS session_id
    FROM `my_project.my_dataset.jjson_table`
  )
  SELECT
    MIN(event_timestamp) AS session_start,
    user_pseudo_id,
    session_id
  FROM sess
  GROUP BY user_pseudo_id, session_id;

  -- Combine source/medium per session
  CREATE OR REPLACE TABLE `my_project.my_dataset.start_user_session_source` AS
  SELECT
    t.session_start,
    t.user_pseudo_id,
    t.session_id,
    CONCAT(
      COALESCE(MAX(IF(rt.kkey = 'source', rt.string_value, NULL)), 'not defined'),
      '/',
      COALESCE(MAX(IF(rt.kkey = 'medium', rt.string_value, NULL)), 'not defined')
    ) AS source_medium
  FROM `my_project.my_dataset.time_user_session` t
  LEFT JOIN `my_project.my_dataset.jjson_table` rt
    ON t.session_start = rt.event_timestamp
   AND t.user_pseudo_id = rt.user_pseudo_id
  GROUP BY t.session_start, t.user_pseudo_id, t.session_id;

  -- Find max steps per user
  SET max_steps = (
    SELECT MAX(cnt)
    FROM (
      SELECT user_pseudo_id, COUNT(*) AS cnt
      FROM `my_project.my_dataset.start_user_session_source`
      GROUP BY user_pseudo_id
    )
  );

  -- Build dynamic pivot
  WHILE i <= max_steps DO
    IF i > 1 THEN
      SET case_expressions = CONCAT(case_expressions, ', ');
    END IF;
    SET case_expressions = CONCAT(
      case_expressions,
      'MAX(IF(rn = ', i, ', source_medium, NULL)) AS step', i
    );
    SET i = i + 1;
  END WHILE;

  -- Final dynamic SQL
  SET sql_text = CONCAT(
    'CREATE OR REPLACE TABLE `my_project.my_dataset.user_paths` AS ',
    'SELECT user_pseudo_id, ', case_expressions, ' FROM (',
      'SELECT user_pseudo_id, source_medium, ',
      'ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY session_start) AS rn ',
      'FROM `my_project.my_dataset.start_user_session_source`',
    ') AS ordered ',
    'GROUP BY user_pseudo_id'
  );

  -- Execute dynamic pivot
  EXECUTE IMMEDIATE sql_text;

END;
```

## Final Results

The final result is a user path table showing each user's journey through different traffic sources:

| user_pseudo_id | step1 | step2 | step3 | step4 | ... |
|----------------|-------|-------|-------|-------|-----|
| 10028188.3857887509 | \<Other\>/referral | (data deleted)/(data deleted) | (data deleted)/(data deleted) | NULL | ... |
| 10101905.8688293836 | google/organic | shop.googlemerchandisestore.com/referral | shop.googlemerchandisestore.com/referral | not defined/not defined | ... |
| 10115718.4867090359 | google/organic | shop.googlemerchandisestore.com/referral | \<Other\>/\<Other\> | NULL | ... |

## Next Steps

And you will answer a logic question: "Cool! What am I going to do with 188 unique source/medium and 6,398 unique User paths (I'm not telling about user distribution between them)?"

Here's the time for Logistic Regression I'm going to tell about next time.

## Contributing

Contributions, issues, and feature requests are welcome!
