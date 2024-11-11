#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "dt.h"
#include "storage_mgr.h"

#define CONSTANT_VALUE_ZERO 0
#define CONSTANT_VALUE -1
#define INITIAL_PAGE_NUMBER CONSTANT_VALUE

int logicClockLoadToMemory = CONSTANT_VALUE_ZERO;
int logicClockOperations = CONSTANT_VALUE_ZERO;


typedef struct BM_PageFrame{
	SM_PageHandle pageData;
	PageNumber pageNumOnDisk;
	bool isDirty;    // Indicates if the Page is Modified
	int activeUsers; //Number of clients currently using the page
	int lifetimeUsers; //Total number of clients that have used the page
	int fetchTime; // Time when the page was loaded in the buffer
	int lastAccessTime; // Time when the page was most recently accessed
}BM_PageFrame;

/*
Author : Riddhi Das
Function: initBufferPool
Description: Here we will be reading and writing from the pages and check how many pages we have at the moment
Also we can check if any new pages are loaded or not so that if there are any then we can proceed to the next function.
*/
RC initBufferPool(BM_BufferPool *const bufferPool, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    initStorageManager();
    
    bufferPool->pageFile = (char *)pageFileName;  // Where we'll be reading from and writing to
    bufferPool->numPages = numPages;              // How many pages we can juggle at once
    bufferPool->strategy = strategy;              // Our plan for swapping pages
    bufferPool->ReadCounts = 0;              // We haven't read anything yet
    bufferPool->WriteCounts = 0;             // We haven't written anything

    BM_PageFrame *pageArray = malloc(sizeof(BM_PageFrame) * numPages);
    if (pageArray == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    for (int i = 0; i < numPages; i++) {
        pageArray[i] = (BM_PageFrame){
            .pageNumOnDisk = INITIAL_PAGE_NUMBER,  // No page loaded yet
            .isDirty = false,                      // Fresh and clean!
            .pageData = NULL,                      // No data yet
            .activeUsers = 0,                      // Nobody's using this page
            .lastAccessTime = 0,                   // Never been accessed
            .fetchTime = 0                         // Never been fetched
        };
    }

    bufferPool->mgmtData = pageArray;

    return RC_OK;
}

/*
Function: shutdownBufferPool
Description: As the name suggests we can check the bufferpool sts and check the flushing of the pools
We can grab an array of page frames and check if the frame contains any page or not and if pages are free for the memory to be allocated.
*/
RC shutdownBufferPool(BM_BufferPool *const bufferPool) {
    if (bufferPool == NULL) {
        return RC_FILE_NOT_FOUND;  // No buffer pool found
    }

    RC flushStatus = forceFlushPool(bufferPool);
    if (flushStatus != RC_OK) {
        // If something went wrong during flushing return the flush status 
        return flushStatus;
    }

    // Grab the array of page frames
    BM_PageFrame *pageFrames = (BM_PageFrame *)bufferPool->mgmtData;
    
    if (pageFrames != NULL) {
        for (int frameIndex = 0; frameIndex < bufferPool->numPages; frameIndex++) {
            // Check if this frame actually contains a page
            if (pageFrames[frameIndex].pageNumOnDisk != INITIAL_PAGE_NUMBER) {
                // Free the memory allocated for the page data
                free(pageFrames[frameIndex].pageData);
                pageFrames[frameIndex].pageData = NULL;  // Avoid dangling pointers
            }
        }
        // Now that all pages are freed, let's free the entire array
        free(pageFrames);
    }

    // Clear out the management data pointer
    bufferPool->mgmtData = NULL;

    return RC_OK;
}

/*
Author : Riddhi Das
Function: writePageToDisk
Description: This function is used to write the pages to the disk.
*/
void writePageToDisk(BM_BufferPool *bufferPool, BM_PageFrame *pageArray, int pageIndexInMemory) {
    // Increment write operation counter
    bufferPool->WriteCounts++;

    SM_FileHandle fileHandle;
    BM_PageFrame *targetPage = &pageArray[pageIndexInMemory];

    // Open the page file
    RC openStatus = openPageFile(bufferPool->pageFile, &fileHandle);
    if (openStatus != RC_OK) {
        printf("Couldn't open the page file for writing.\n");
        return;
    }

    // Validate page data
    if (targetPage->pageData == NULL) {
        printf("Invalid page data so skipping the write operation.\n");
        closePageFile(&fileHandle);
        return;
    }

    // Write the page to disk
    RC writeStatus = writeBlock(targetPage->pageNumOnDisk, &fileHandle, targetPage->pageData);
    if (writeStatus != RC_OK) {
        printf("Writing the page to disk is not possible.\n");
        closePageFile(&fileHandle);
        return;
    }

    // Clean up and update page status
    closePageFile(&fileHandle);
    targetPage->isDirty = false;  // Page is now clean
    printf("Page successfully written to disk\n");
}

/*
Author : Riddhi Das
Function: forceFlushPool
Description: This function retrieves the array of page frames from the buffer pool and checks if the page needs flushing or not
If the page needs flushing it writes the page to the disk and then marks the page clean.
*/
RC forceFlushPool(BM_BufferPool *const bufferPool) {
    // Retrieve the array of page frames from the buffer pool
    BM_PageFrame *pageFrames = (BM_PageFrame *)bufferPool->mgmtData;

    // Iterate through all pages in the buffer pool
    for (int frameIndex = 0; frameIndex < bufferPool->numPages; frameIndex++) {
        BM_PageFrame *currentFrame = &pageFrames[frameIndex];

        // Check if the page needs flushing (dirty and not in use)
        bool needsFlushing = currentFrame->isDirty && (currentFrame->activeUsers == 0);

        if (needsFlushing) {
            // Write the page to disk
            writePageToDisk(bufferPool, pageFrames, frameIndex);

            // Mark the page as clean
            currentFrame->isDirty = false;

            #ifdef DEBUG
            printf("Flushed page to disk\n", currentFrame->pageNumOnDisk);
            #endif
        }
    }

    // All dirty pages have been flushed successfully
    return RC_OK;
}

/*
Author : Riddhi Das
Function: markDirty
Description: This function retrieves the array of page frames from the buffer pool and then searches for the page in the pool.
If the page is found then it exits the loops
else If page is not found then cant make it dirty
else Makes the page dirty
*/
RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    // Retrieve the array of page frames from the buffer pool
    BM_PageFrame *pageFrames = (BM_PageFrame *)bufferPool->mgmtData;

    // Search for the page in the buffer pool
    int targetFrameIndex = -1;
    int i = 0;

    while (i < bufferPool->numPages && targetFrameIndex == -1) {
        if (pageFrames[i].pageNumOnDisk == page->pageNum) {
            targetFrameIndex = i;
        }
        i++;
    }

    // Check if the page was found in the buffer pool
    if (targetFrameIndex == -1) {
        // Page not found, log the error and return
        printf("Page %d not found in buffer pool so can't mark it dirty.\n", page->pageNum);
        return RC_FILE_NOT_FOUND;
    }

    // Mark the page as dirty
    pageFrames[targetFrameIndex].isDirty = true;

    // Log the action for debugging purposes
    printf("Page %d has been marked as dirty.\n", page->pageNum);

    return RC_OK;
}


// Author : Sanketkumar Patel 
// Function : PageToBePinnedAsPerStrategy (Supporting Fuction to pinPage)
// Description : Determines which page to evict from the buffer pool based on the selected page replacement strategy (FIFO or LRU).
// Inputs : BM_BufferPool *bm (buffer pool management structure)
int PageToBePinnedAsPerStrategy(BM_BufferPool *const bm) {

    // Access page frames from the buffer pool management data.
    BM_PageFrame *CurrentBMpageFrames = (BM_PageFrame *)bm->mgmtData;

    // This variable will hold the index of the page selected for eviction.
    int pageIndexToEvict = -1;

    // Initialize minimum time variables to track the oldest page according to the eviction strategy.
    int selectedTime = INT32_MAX;

    // Iterate through all pages in the buffer pool.
    for (int i = 0; i < bm->numPages; i++) {
        // Only consider unpinned pages (activeUsers == 0).
        if (CurrentBMpageFrames[i].activeUsers == 0) {
            switch (bm->strategy) {
                case RS_FIFO: { // FIFO: evict the page with the earliest fetch time.
                    if (CurrentBMpageFrames[i].fetchTime < selectedTime) {
                        selectedTime = CurrentBMpageFrames[i].fetchTime;
                        pageIndexToEvict = i;
                    }
                    break;
                }
                case RS_LRU: { // LRU: evict the page with the oldest last access time.
                    if (CurrentBMpageFrames[i].lastAccessTime < selectedTime) {
                        selectedTime = CurrentBMpageFrames[i].lastAccessTime;
                        pageIndexToEvict = i;
                    }
                    break;
                }
                default: {
                    printf("Error: Replacement strategy %d not supported.\n", bm->strategy);
                    return -1;
                }
            }
        }
    }

    // If a valid page has been found for eviction, proceed with eviction.
    if (pageIndexToEvict != -1) {
        // If the page is dirty, write it back to disk.
        if (CurrentBMpageFrames[pageIndexToEvict].isDirty) {
            writePageToDisk(bm, CurrentBMpageFrames, pageIndexToEvict);
        }
        // Free the memory associated with the page's data.
        free(CurrentBMpageFrames[pageIndexToEvict].pageData);
    }

    // Return the index of the page selected for eviction.
    return pageIndexToEvict;
}

// Author: Sanketkumar Patel
// Function: pinPage
// Description: This function pins a page from the page file to memory. 
//              If the requested page is already in memory, it returns a pointer to the page. 
//              If the page is not in memory, it loads the page from disk, possibly evicting another page based on the replacement strategy.
// Inputs: BM_BufferPool *bm (buffer pool to manage the pages), BM_PageHandle *page (handle to hold the pinned page data), 
//         PageNumber pageNum (the page number to be pinned)
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {

    logicClockOperations++; // Keep track of the access time by incrementing a logical clock.

    // Step 1: Open the page file and ensure it can handle the requested page number.

    // Open the file associated with the buffer pool to access the requested page.
    SM_FileHandle fileHandle;
    RC rc = openPageFile(bm->pageFile, &fileHandle);
    if (rc != RC_OK) {
        return rc; // Return if the file cannot be opened.
    }

    // Ensure the file has enough capacity to include the requested page number.
    rc = ensureCapacity(pageNum + 1, &fileHandle);
    if (rc != RC_OK) {
        closePageFile(&fileHandle); // Close the file if there’s an issue with capacity.
        return rc; // Return the error code if the file cannot hold the requested page.
    }

    // Assign the page number to the page handle once we know the file can store the page.
    page->pageNum = pageNum;

    // Step 2: Look for the requested page in memory.

    BM_PageFrame *CurrentBMpageFrames = (BM_PageFrame *)bm->mgmtData; // Get the list of page frames in the buffer pool.
    int IndexOfDesiredPageinMemory = -1; // Variable to track if the page is already in memory.

    // Check if the requested page is already loaded in memory by searching through the page frames.
    for (int i = 0; i < bm->numPages; i++) {
        if (CurrentBMpageFrames[i].pageNumOnDisk == pageNum) { // If the page is found in memory.
            IndexOfDesiredPageinMemory = i; // Save the index where it’s found.
            break;
        }
    }

    // Step 3: If the page is already in memory, update its status and return.

    if (IndexOfDesiredPageinMemory != -1) {
        // Update the number of users pinning the page, set the page data, and update access time.
        CurrentBMpageFrames[IndexOfDesiredPageinMemory].activeUsers++; // Increment the number of users pinning this page.
        page->data = CurrentBMpageFrames[IndexOfDesiredPageinMemory].pageData; // Point to the page data in memory.
        CurrentBMpageFrames[IndexOfDesiredPageinMemory].lastAccessTime = logicClockOperations; // Update access time.
        return RC_OK; // Successfully pinned the page in memory.
    }

    // Step 4: If the page is not in memory, load it from disk.

    bm->ReadCounts++; // Increment the counter for read operations (page load from disk).

    int IndexofPageWhereToPin = -1; // This will store the index where we load the new page in memory.

    // Step 4.1: Look for an empty frame to load the new page if there is one.
    int i=0;
    while (i < bm->numPages){
        if (CurrentBMpageFrames[i].pageNumOnDisk == INITIAL_PAGE_NUMBER) { // Check for an empty page frame.
            IndexofPageWhereToPin = i; // Use this empty frame to load the page.
            break;
        }
        i++;
    }

    // Step 4.2: If no empty page frame is found, use the page replacement strategy to find a frame to replace.
    if (IndexofPageWhereToPin == -1) {
        IndexofPageWhereToPin = PageToBePinnedAsPerStrategy(bm); // Call the replacement strategy to get the index of the page to evict.
    }

    //If replacement startegy returns -1 then it means all pages in memory are pinned.
    if (IndexofPageWhereToPin == -1) {
        printf("All Pages in Buffer Pool are occupied by users there is no possibility to pin the page");
        return RC_BUFFERPOOLFULL;
    }


    // Step 4.3: Allocate memory to store the new page data.
    CurrentBMpageFrames[IndexofPageWhereToPin].pageData = (SM_PageHandle)malloc(PAGE_SIZE);
    if (CurrentBMpageFrames[IndexofPageWhereToPin].pageData == NULL) {
        closePageFile(&fileHandle); // If memory allocation fails, close the file and return an error.
        return RC_FILE_HANDLE_NOT_INIT; // Return an error code for failed memory allocation.
    }

    // Step 4.4: Read the requested page from the disk into the allocated memory.
    rc = readBlock(pageNum, &fileHandle, CurrentBMpageFrames[IndexofPageWhereToPin].pageData);
    
    // If reading from the disk fails, free the allocated memory and return the error.
    if (rc != RC_OK) {
        free(CurrentBMpageFrames[IndexofPageWhereToPin].pageData); // Free the memory that was allocated.
        closePageFile(&fileHandle); // Close the file.
        return rc; // Return the error code from the read operation.
    }

    // Step 4.5: Close the file once the page is loaded into memory.
    closePageFile(&fileHandle);

    // Step 5: Initialize the page frame with details about the newly loaded page.
    BM_PageFrame *NewpageFrame = &CurrentBMpageFrames[IndexofPageWhereToPin];
    NewpageFrame->pageNumOnDisk = pageNum; // Store the page number that was loaded.
    NewpageFrame->isDirty = false; // Set the page as clean since it was just loaded from disk.
    NewpageFrame->activeUsers = 1; // Set the active user count (this page is pinned now).
    NewpageFrame->lastAccessTime = logicClockOperations; // Update the last access time.
    NewpageFrame->fetchTime = ++logicClockLoadToMemory; // Record when the page was loaded into memory.
    page->data = CurrentBMpageFrames[IndexofPageWhereToPin].pageData; // Set the page handle’s data pointer to the loaded page.

    return RC_OK; // Successfully pinned the new page in memory.
}

// Author: Sanketkumar Patel
// Function: unpinPage
// Description: This function unpins a page from the buffer pool, reducing its active user count.
//              If the page is not found in the buffer pool or is already unpinned, it returns an error. 
//              Otherwise, it decreases the pin count and marks the page as successfully unpinned.
// Inputs: BM_BufferPool *bm (the buffer pool containing the page), BM_PageHandle *page (the page to be unpinned)
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    // Retrieve the list of page frames associated with the buffer pool.
    BM_PageFrame *currentPageFrames = (BM_PageFrame *)bm->mgmtData;

    // Set a default value for the index of the page to unpin, indicating no page has been found yet.
    int indexOfPageToUnpin = -1;

    // Scan through the buffer pool to locate the page corresponding to the given page number.
    int i = 0;
    while (i < bm->numPages) {
        if (currentPageFrames[i].pageNumOnDisk == page->pageNum) {  // If the page is found in the pool.
            indexOfPageToUnpin = i;  // Capture the index of the located page.
            break;  // Stop searching once the page is found.
        }
        i++;  // Move to the next page frame in the pool.
    }

    // Check if the page is not in the buffer pool.
    if (indexOfPageToUnpin == -1) {
        printf("Error: Page %d not found in the buffer pool.\n", page->pageNum);
        return RC_PAGE_NOT_FOUND_IN_BUFFERPOOL;  // Error: page not found in memory.
    }

    // Check if the page is already unpinned (i.e., no active users are holding the page).
    if (currentPageFrames[indexOfPageToUnpin].activeUsers == 0) {
        printf("Error: Page %d is already unpinned.\n", page->pageNum);
        return RC_PAGE_ALREADY_UNPINNED;  // Error: page is already unpinned.
    }
    
    printf("Unpinning page %d\n", page->pageNum);
    // Decrement the active user count for the page.
    currentPageFrames[indexOfPageToUnpin].activeUsers--;  // Reduce the number of clients holding the page.
    return RC_OK;  // Unpin operation completed successfully.
}

// Author: Sanketkumar Patel
// Function: forcePage
// Description: This function forces a specified page from the buffer pool to be written back to disk. 
//              It checks if the requested page exists in memory and whether it has been modified (is dirty). 
//              If the page is found and dirty, it writes the page back to the disk. 
//              If the page is not found or not dirty, it returns appropriate error codes.
// Inputs: BM_BufferPool *bm (buffer pool containing the pages), 
//         BM_PageHandle *page (handle to the page to be written back).
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    // Obtain the array of page frames linked to the buffer pool.
    BM_PageFrame *currentPageFrames = (BM_PageFrame *)bm->mgmtData;

    // Initialize the variable to track the index of the page to be forced, defaulting to -1 (not found).
    int indexOfPageToForce = -1;

    // Iterate through the buffer pool to find the page that matches the specified page number.
    int i = 0;
    while (i < bm->numPages) {
        if (currentPageFrames[i].pageNumOnDisk == page->pageNum) {  // Check for a match in the page pool.
            indexOfPageToForce = i;  // Store the index of the identified page.
            break;  // Exit the loop once the page is located.
        }
        i++;  // Move to the next page frame in the buffer pool.
    }

    // Determine if the page exists within the buffer pool.
    if (indexOfPageToForce == -1) {
        printf("Error: Page %d not found in the buffer pool.\n", page->pageNum);
        return RC_PAGE_NOT_FOUND_IN_BUFFERPOOL;  // Error: the specified page is absent.
    }

    // Verify whether the page has been modified (is dirty).
    if (!currentPageFrames[indexOfPageToForce].isDirty) {
        printf("Page %d is not dirty in memory, no need to force write.\n", page->pageNum);
        return RC_PAGE_NOT_DIRTY;  // Error: the page does not require writing.
    }

    printf("Writing page %d back to disk in forcePage\n", page->pageNum);
    writePageToDisk(bm, currentPageFrames, indexOfPageToForce);  // Persist the changes to disk.
    return RC_OK;  // Page successfully written back to disk.
}

// Author: Hetanshi Darbar 
// Function: getFrameContents
// Description: This function retrieves the contents of the page frames in the buffer pool.
//              It returns an array of PageNumbers, indicating which pages are currently stored in memory.
//              If a page frame is uninitialized, it is represented as NO_PAGE in the result.
// Inputs: BM_BufferPool *bm (pointer to the buffer pool structure).
// Outputs: PageNumber * (an array containing the page numbers of the pages currently in the buffer).
PageNumber *getFrameContents(BM_BufferPool *const bm) {

    // Retrieve the total count of pages allocated in the buffer pool.
    int BMPages = bm->numPages;

    // Create a new array to hold the page numbers.
    PageNumber *frameNumbers = malloc(sizeof(PageNumber) * BMPages);

    // Obtain a pointer to the page frames within the buffer pool's management structure.
    BM_PageFrame *currentPageFrames = (BM_PageFrame *)bm->mgmtData;

    // Iterate over all page frames to fill the frameNumbers array.
    for (int index = 0; index < bm->numPages; index++) {
        // Determine if the current page frame contains a valid page number.
        if (currentPageFrames[index].pageNumOnDisk != INITIAL_PAGE_NUMBER) {
            frameNumbers[index] = currentPageFrames[index].pageNumOnDisk; // Assign the valid page number.
        } else {
            frameNumbers[index] = NO_PAGE; // Set to NO_PAGE if the frame is uninitialized.
        }
    }

    return frameNumbers; // Deliver the array that contains the page numbers.
}

// Author: Hetanshi Darbar
// Function: getDirtyFlags
// Description: This function retrieves the dirty flags for each page in the buffer pool. 
//              It allocates an array of boolean values where each entry indicates 
//              whether the corresponding page in the buffer pool has been modified (is dirty).
// Inputs: BM_BufferPool *bm (buffer pool containing the pages).
// Outputs: bool* (pointer to an array of boolean values representing the dirty status 
//                  of each page; returns NULL if memory allocation fails).
bool *getDirtyFlags(BM_BufferPool *const bm) {
    // Determine the number of pages available in the buffer pool.
    const int pageCount = bm->numPages;

    // Allocate memory for an array to hold the dirty status of each page.
    bool *dirtyFlagsArray = malloc(sizeof(bool) * pageCount);


    if (dirtyFlagsArray == NULL) {
        return NULL; // Handle memory allocation failure.
    }

    // Cast the management data to access the page frames.
    BM_PageFrame *currentPageFrames = (BM_PageFrame *)bm->mgmtData;

    // Loop through each page frame and populate the dirtyFlagsArray.
    for (int index = 0; index < pageCount; index++) {
        // Directly copy the dirty status from the page frame to the array.
        dirtyFlagsArray[index] = currentPageFrames[index].isDirty;
    }

    // Return the array containing the dirty flags of the pages.
    return dirtyFlagsArray;
}

// Author: Hetanshi Darbar
// Function: getFixCounts
// Description: This function retrieves the count of active users (fix counts) for each page in the buffer pool. 
//              It checks for the validity of the buffer pool before proceeding. If valid, it allocates memory 
//              for an array to store the fix counts, populates it by accessing the page frames, and returns 
//              the populated array. If the buffer pool is not initialized or memory allocation fails, it returns NULL.
// Inputs: BM_BufferPool *bm (pointer to the buffer pool from which to retrieve fix counts)
int *getFixCounts(BM_BufferPool *const bm) {
    // Verify the validity of the buffer pool to ensure safe access.
    if (bm == NULL) {
        return NULL;  // Exit and return NULL if the buffer pool hasn't been initialized.
    }

    // Create an array to hold the count of fixed pages, allocating sufficient memory.
    int *fixCountsArray = (int *)malloc(sizeof(int) * bm->numPages);
    if (fixCountsArray == NULL) {
        return NULL;  // Exit and return NULL if the memory allocation is unsuccessful.
    }

    // Retrieve the pointer to the page frames stored in the management structure of the buffer pool.
    BM_PageFrame *currentPageFrames = (BM_PageFrame *)bm->mgmtData;

    // Loop through each page frame to fill the array with the count of active users.
    for (int index = 0; index < bm->numPages; index++) {
        fixCountsArray[index] = currentPageFrames[index].activeUsers;  // Assign the active user count for each page to the array.
    }

    return fixCountsArray;  // Return the array containing the counts of fixed pages.
}

// Author: Hetanshi Darbar
// Function: getNumReadIO
// Description: This function retrieves the number of read I/O operations that have been performed on the buffer pool. 
//              It first checks if the buffer pool pointer is valid. If not, it logs an error message and returns an appropriate error code.
// Inputs: BM_BufferPool *bm (buffer pool to manage the pages)
// Returns: int - the number of read I/O operations or an error code if the buffer pool is not initialized.
int getNumReadIO(BM_BufferPool *const bm) {

    // Check if the buffer pool pointer is NULL or invalid.
    if (bm == NULL) {
        printf("Error: Buffer pool is not initialized.\n");
        return RC_FAILED_TO_GET_READ_NUMBER_BUFFERPOOL_DOES_NOT_INT;  // Return an error code or handle the error as needed.
    }
    return bm->ReadCounts;
}

// Author: Hetanshi Darbar 
// Function: getNumWriteIO
// Description: This function retrieves the number of write I/O operations that have been performed on the buffer pool. 
//              It checks for the validity of the buffer pool pointer before accessing its members. If the pointer is invalid, it logs an error and returns an appropriate error code.
// Inputs: BM_BufferPool *bm (buffer pool to manage the pages)
// Returns: int - the number of write I/O operations or an error code if the buffer pool is not initialized.
int getNumWriteIO(BM_BufferPool *const bm) {

    // Check if the buffer pool pointer is NULL or invalid.
    if (bm == NULL) {
        printf("Error: Buffer pool is not initialized.\n");
        return RC_FAILED_TO_GET_WRITE_NUMBER_BUFFERPOOL_DOES_NOT_INT;  // Return an error code or handle the error as needed.
    }
    return bm->WriteCounts;
}