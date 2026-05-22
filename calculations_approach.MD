## Brief Summary of Statistical Calculations

### 1. **Maximum Value**
- Takes the highest numerical value from all **valid samples** (0-100 range)
- Returns `None` if no valid samples exist

### 2. **Minimum Value**
- Takes the lowest numerical value from all **valid samples** (0-100 range)
- Returns `None` if no valid samples exist

### 3. **Mean (Average)**
- Sum of all valid samples divided by count of valid samples
- Formula: `average = sum(valid_samples) / len(valid_samples)`
- Returns `None` if no valid samples exist

### 4. **Sample Standard Deviation**
- Uses **Bessel's correction** (divides by N-1, not N)
- Formula: `std_dev = sqrt( Σ(x - mean)² / (n-1) )`
- Returns `0.0` if only 1 valid sample (undefined)
- Returns `None` if no valid samples exist

### 5. **Missing Samples Count**
- Counts all `None` values in the block
- These are sensor readings that completely failed

### 6. **Corrupted Samples Count**
- Counts values that are:
  - Non-numeric strings (e.g., "ERROR", "INVALID")
  - Numbers outside [0, 100] range (e.g., -1, 101)
- Excludes `None` values (those are "missing")

### Data Classification Flow
```
Sample → Is None? → YES → Missing Count +1
         ↓ NO
         Is 0-100? → NO → Corrupted Count +1  
         ↓ YES
         Valid Sample → Used for max/min/mean/std dev
```

### Example
Input: `[10, None, 25, "ERROR", 30, -1, 40]`

| Statistic | Value | Calculation |
|-----------|-------|-------------|
| Max | 40 | Highest valid |
| Min | 10 | Lowest valid |
| Mean | 26.25 | (10+25+30+40)/4 |
| Std Dev | 12.5 | sqrt(Σ(x-26.25)²/3) |
| Missing | 1 | (None) |
| Corrupted | 2 | ("ERROR", -1) |
| Valid | 4 | (10,25,30,40) |
