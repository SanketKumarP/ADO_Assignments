# Assignment 2 - Buffer Manager Implementation
## Group 44

### Members:

### Sanketkumar Patel (A20523237)
Responsible for collectively managing  page pinning, unpinning, forcing, and eviction in a buffer pool, implementing page replacement strategies and handling disk I/O operations to maintain an efficient memory-to-disk page management system. Implemented functions:

pinPage, unpinPage, PageToBePinnedAsPerStrategy, forcePage

### Hetanshi Deepakkumar Darbar (A20552832)
Responsible for collectively providing information about the buffer pool's state, returning arrays of frame contents, dirty flags, and fix counts, as well as the total read and write I/O operations performed. Implemented functions:

getFrameContents, getDirtyFlags, getFixCounts, getNumReadIO, getNumWriteIO

### Riddhi Das (A20582829)
Responsible for collectively managing a buffer pool's lifecycle, including initialization, page management, dirty page tracking, flushing to disk, and shutdown, while handling read/write operations and implementing page replacement strategies.. Implemented functions:

initBufferPool, shutdownBufferPool, writePageToDisk, forceFlushPool, markDirty

## Steps to follow to run the code
### 1. Clone the Repository and Open Folder assign2:
Clone the project repository from GitHub and Open Folder assign2

### 2. Clean the Build
To remove all generated object files and executables before executing project, run following command in the terminal:

make clean

### 3. To Compile the Project
To compile the project, run the following command in the terminal:

make

### 4. To run the test cases
To run the compiled program, use following command in the terminal:

make run

### 5. Clean the Build (Repeating step2)
To remove all generated object files and executables, run following command in the terminal:

make clean


## Notes:

- Replacement Methods: This project supports only two replacement methods: FIFO and LRU.

- We have made several modifications:

1. Error Codes: Added a few error codes to dberror.h.
2. Buffer Pool Enhancement: Extended BM_BufferPool by adding two counter variables to track read and write operations. This also resulted in changes to buffer_mgr.h.

