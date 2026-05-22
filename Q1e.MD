## Runtime Configurable Block Size - Summary [implemented]

The block size can be changed at runtime through a dedicated API endpoint. When a change request is received, the system first acquires a lock to safely modify the shared state. If the `force` flag is set to true, the current incomplete block is processed immediately before any size change. Then the system evaluates the new size relative to the current block. If the new size is larger than the current block, the system simply updates the block size and continues accumulating samples to the new target. If the new size is smaller and the current block already contains more samples than the new size, the system splits the existing block into multiple smaller blocks, processing each chunk as a complete block, and keeps any remaining samples for the next block. If the current block is empty, the size change happens instantly. All operations are protected by an asynchronous lock to prevent race conditions with the consumer task that is simultaneously adding samples to the block. The result of the change operation is returned as a JSON response showing the old size, new size, how many blocks were created, and any remaining samples. This design allows operators to adjust the processing granularity on the fly without stopping the system or losing data.




