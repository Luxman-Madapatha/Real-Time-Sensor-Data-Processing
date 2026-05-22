Part (b): Performance Analysis Under High Data Rate
1. Impact of High Data Arrival Rate
FactorImpactProcessor UtilizationCPU must compute stats (max, min, mean, std dev) faster than data arrives. At very high rates, the CPU may be continuously busy, leaving no idle time, causing overload.Memory RequirementsIf samples arrive faster than they are processed, unprocessed data accumulates in a buffer. Memory demand grows linearly with the backlog, potentially causing buffer overflow.System LatencyProcessing latency (time from sample arrival to result output) increases as the queue grows. In a real-time system, this violates timing constraints and makes outputs stale.
2. Consequences of Falling Behind

Data loss: Overflow causes samples to be silently dropped, corrupting statistics.
Stale outputs: Results are computed on old data, making them meaningless for real-time decisions.
System crash: Uncontrolled memory growth can exhaust RAM, causing the OS to kill the process.
Cascading failure: Downstream systems relying on processed data also malfunction.
