# Data Dimensions
## Definition
Attributes of an entity (e.g. user's birthday, favorite food, name, etc)
## Types
- Slowy changing (e.g favorite food, address, etc)
- Fixed (Name, birthday, etc)

# Identifying the consumer
## Types of Consumer and their data needs
### Data Analysts/ Scientists
- Easy to query data
- Not complex types
### Data Engineers
- Compact
- Complex types are no issue
### ML Models
- Dependent on model Type
### Customers
- Easy interpretable data

# Data Modeling
## Types
### OLTP (Online Transaction Processing)
- Mostly for software engineer and developers with lots of joins and foreign keys optimising for speed
### OLAP (Online Analytical Processing)
- Optimised for large data while minimizing JOINs
### Master Data
- Middle Ground
- Can have deduplication
- Captures all information about various entities

# Cumulative table Design
Design Tables to hold all of history. Allows tracking of all changes throuhgout time. e.g. Full outer join of yesterday and today to create a cumulative table. Continuing to do this can create a history all states and changes can then be identified at a later time.
## Advantages
- Historical Analysis
- Allows to identify transition of state
## Drawbacks
- Difficult backfilling process (Only sequentially possible)
- Some irrelevant data could be carried forward, e.g. Deleted users or inactive accounts

# Complex Data Types
### Struct
- Various keys with values assigned to them (Like an json object)
- Any value type allowed for each key
### Map
- Key value pairs
- All values have to be of the same type
### Array
- Ordered list
- All values of the same type

# Idempotency
## What is it?
- Ability for pipelines to produce same results in similar conditions
- No matter:
    - When run
    - How many times
    - As long inputs remain same

## Why so difficult
- Silent Failures
    - Can only be detected while checking data discrepencies and inconsistency

## Possible Reasons
- Inserting same data again in future runs of pipelines when there are no changes made (Solution: INSERT OVERRIDE instead of INSERT)
- Not having valid limits, for e.g. Start_date without any End_date
- Running pipelines without all needed inputs
- Cumulative pipelines running in parallel instead of sequentially
- Using latest partitioned data without properly labelled SCD (Slowly changing dimensions) tables

## Problems caused by not having idempotent pipleines
- Backfilling data produces incosistent results
- Difficulty in Unit Testing (False Positives)

# SCD (Slowly Changing Dimensions)
## What is it?
Data dimension that changes over time but not as regularly. Can cause issues with indempotency. For e.g. age, food preferences, beliefs, etc.

## How to model it?
- Latest Snapshot: One snapshot of the latest data. BIG NO NO! Makes it impossible to backfill
- Daily snapshots. Easy to process since storage is mostly cheap

## Loading SCD Data
- 1 single query to process all of history
- Cumulative design but cannot be parallelised

# Additive dimensions
## What is it?
- Dimensions who can have sub groups and each group is mutually exclusive of the other. This allows the total to be calculated as a sum total of each individual sub group.
- Some dimensions can be additive over a specific unit of time, if and only if, the values are mutually exclusive of any other group for that specific unit of time.