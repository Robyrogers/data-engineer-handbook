# What is a fact?
- Atomic events that happened in time
- Doesn't change
## Difficulties
- Large volume (Tracks every event, for e.g. every step by a fitbit tracker)
- Requires context for analysis
- Lots of duplication
## Types
- Normalized (Requires joins, therefore less data duplication)
- Denormalized (Contains all dimensions and doesn't need any joins but introduces duplication)
## Properties
- High Quality (Better than row logs)
- Smaller in size compared to raw logs
- Easy to parse or query columns
## Working with large volume data
- Sampling (Suitable for metric data with low level of required accuracy)
- Bucketing
    - Based on identity or specific dimension
    - Bucketed join instead of shuffle join
    - Sorted-merge Bucket (SMB)
## Longevity or retention
- Depends on company policy, legality and cost
- Mostly 60 or 90 days
# Fact vs Dimensions
Can be blury. Hard to tell apart in certain situations.
## Dimensions
- Used to do GROUP BY for analytics
- Generally low cardinality but depends
- Captured as snapshots
## Fact
- Can be aggregated for analytics
- Generally large volume
- Generally events and logs