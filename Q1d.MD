Part (d): System Design Improvements
Technique 1: Double Buffering (Ping-Pong Buffers)
Use two alternating buffers. While one buffer is being filled with incoming sensor data, the other is being processed by the statistics engine. When the filling buffer is full, they swap roles.

Improvement: Decouples data acquisition from processing — data collection never has to wait for computation to finish, drastically reducing latency and preventing data loss at high rates.
Reliability: Even if computation takes longer than one block period, the second buffer absorbs the incoming data without loss.

Technique 2: Circular Queue with Overflow Detection
Implement a circular (ring) buffer between the data ingestion thread and the processing thread, with a configurable maximum size.

Improvement: Smooths out bursty data arrival. The queue absorbs temporary spikes in data rate. The processing thread consumes from the queue at a steady pace.
Robustness: When the queue reaches capacity, the system can either drop the oldest sample (with a logged warning) or apply back-pressure to the sensor interface — both are preferable to a crash.
