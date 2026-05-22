Part (c): Handling Missing, Corrupted, and Delayed Data
1. Handling Strategies
Missing Samples:

Detect None / sentinel values in the stream.
Options: skip them (reduce N), or interpolate using adjacent valid values (linear interpolation).
Count them and include in the report so the operator knows accuracy is reduced.

Corrupted / Invalid Data:

Validate every sample: must be an integer in [0, 100].
Values outside range (e.g., -1, 999) or non-numeric values are flagged as corrupted.
Exclude from statistical calculations; log the count per block.

Delayed Arrival:

Use a timeout mechanism: if a block is not completed within a time window T, process whatever samples have arrived.
Mark the block as incomplete and report the number of received vs. expected samples.
Use timestamps on each sample to detect out-of-order arrivals and reorder them within a tolerance window.

2. Effect on Statistical Accuracy
IssueEffect on StatisticsMissing samplesReduces effective N; mean/std dev may shift if missing is not randomCorrupted samplesOutliers can drastically skew max, min, and std deviation if not removedDelayed/out-of-order dataBlock boundaries misalign — samples from one time window end up in another, producing misleading block-level statistics
