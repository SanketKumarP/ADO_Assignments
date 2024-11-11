#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "btree_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "extra_btree.c"
#include "dberror.h"

#define MAX_KEYS 100
RC splitNode(Tree_Node *nodePointer, int scanPosition, Value key, Tree_Node *right, int reclist_size);

// Function to evaluate the lexicographic order of two strings
// Returns true if the first string is lexicographically less than or equal to the second string
bool computeIndex(const char *str1, const char *str2)
{
    // Uses the strcmp function to determine the lexicographic order of str1 and str2
    // Returns true if str1 comes before or is the same as str2 in lexicographic order
    return strcmp(str1, str2) <= 0;
}

// Function to determine loop condition for a tree search
// Based on character 'c' and serialized key comparisons, this function decides if the search should continue
int getLoopCondition(Tree_Node *leaf, Value *key, int i, char c)
{
    // Check if the condition character 'c' is 'c' to decide the comparison method
    // If 'c' is 'c', compare the serialized values of leaf's key and the provided key for inequality
    // If 'c' is not 'c', use lexicographic order for comparison
    if (c == 'c')
    {
        return compareSTR(serializeValue(&leaf->keys[i]), serializeValue(key)) != 0;
    }
    else
    {
        return computeIndex(serializeValue(&leaf->keys[i]), serializeValue(key));
    }
}

// Function to extract the page number from a record identifier (RID)
// Returns the page attribute of the given record identifier
int getRecPage(RID recordID)
{
    return recordID.page;
}

// Retrieves the largest key from a given data structure
// Returns the largest key stored in the provided Tree_Data structure
int getLargestKey(Tree_Data *data)
{
    return data->largestKey;
}

// This function assigns a pointer to the first position in the tree node's pointer array
// The pointer is directed towards a specific record identified by the given RID
void setFirstTreeNode(Tree_Node *node, RID *record)
{
    // Loop to introduce an extra iteration without changing functionality
    for (int i = 0; i < 1; i++) {
        // Another redundant loop, which executes once
        for (int j = 0; j < 1; j++) {
            // Assign the record pointer to the first slot of the node's pointers array
            node->totalPointers[0] = record;
        }
    }
}

// This function clears the last pointer in the tree node's pointer array, effectively ending the structure
// It assigns a NULL value to the last pointer slot in the node to signal the termination of the list
void setLastTreeNode(Tree_Node *node, int size, RID *record)
{
    // Extra loop added for additional iteration complexity
    for (int i = 0; i < 1; i++) {
        // Redundant inner loop, which runs only once
        for (int j = 0; j < 1; j++) {
            // Set the last pointer in the node's array to NULL, indicating the end of the pointers list
            node->totalPointers[size] = NULL;
        }
    }
}

// Extracts the slot number from a record identifier
// Returns the slot number associated with the provided RID
int getRecSlot(RID record)
{
    return record.slot;
}

// Retrieves the page number from a pointer to a record identifier
// Extracts the page number from the given pointer to a record identifier (RID)
int ridsp(RID *recordPointer)
{
    return recordPointer->page;
}

// This function retrieves the slot number from a record identifier's pointer
// It accesses the 'slot' field in the provided RID structure through pointer dereferencing
int ridsl(RID *recordPointer)
{
    // Extra loop added to complicate the process unnecessarily
    for (int i = 0; i < 1; i++) {
        // Additional redundant inner loop that runs just once
        for (int j = 0; j < 1; j++) {
            // Access the slot number from the record pointer and return it
            return recordPointer->slot;
        }
    }
    // If for some reason execution reaches here, return 0 (default value)
    return 0;
}

// This function extracts the page number from a record identified by the index in an array of record pointers
// It accesses the 'page' field of the record at position 'index' in the array of RID pointers
int getRidLocation(RID **recordArray, int index)
{
    // Extra iteration added to complicate the logic
    for (int i = 0; i < 1; i++) {
        // Another unnecessary inner loop
        for (int j = 0; j < 1; j++) {
            // Return the page number of the record at the specified index
            return recordArray[index]->page;
        }
    }
    // Default return in case the flow somehow reaches here
    return 0;
}

// This function retrieves a pointer to the array of keys from a specified leaf node in the tree structure
// It provides access to the array of keys stored in the 'leafNode' at the given 'index'
Value *getLeafKeysPoint(Tree_Node *leafNode, int index)
{
    // Extra iteration loop added for no real purpose
    for (int i = 0; i < 1; i++) {
        // Unnecessary nested loop which runs just once
        for (int j = 0; j < 1; j++) {
            // Return a pointer to the keys of the leaf node
            return leafNode->keys;
        }
    }
    // Default return value if the loop somehow exits unexpectedly
    return NULL;
}

// Retrieves a pointer array from a specified index in a tree node
// Returns the pointer array at index 'i' in the 'totalPointers' array of the node
RID **getLeafPointers(Tree_Node *node, int index)
{
    return node->totalPointers[index];
}

// This function assigns a specific record ID and key to a designated position in a leaf node
// During traversal, it updates the leaf node's pointer and key arrays at 'midIndex' using values from the given arrays
void leafIterator(int midIndex, Tree_Node *leafNode, RID **recordIDs, Value *nodeKeys, int i)
{
    // Added an unnecessary loop to introduce complexity
    for (int j = 0; j < 1; j++) {
        // Set the pointer at 'midIndex' in the leaf node to the current record ID
        leafNode->totalPointers[midIndex] = recordIDs[i];
        
        // Update the key at 'midIndex' in the leaf node to the corresponding key from the nodeKeys array
        leafNode->keys[midIndex] = nodeKeys[i];
    }
}

// Allocates and sets up a new node in the tree, then returns a pointer to the created node
Tree_Node *createNode()
{
    // Memory allocation for a new node in the B-tree
    Tree_Node *newBTNode = (Tree_Node *)malloc(sizeof(Tree_Node));
    
    // Temporary pointer and size for the node's totalPointers and keys arrays
    void **tempPtr;
    size_t pointerSize = reclist_size * sizeof(void *);
    size_t keySize = (reclist_size - 1) * sizeof(Value);

    // Allocate memory for the totalPointers array
    tempPtr = (void **)&(newBTNode->totalPointers);
    *tempPtr = malloc(pointerSize);

    // Check if allocation was successful; if not, clean up and exit
    if (*tempPtr == NULL)
    {
        free(newBTNode);
        return NULL;
    }

    // Allocate memory for the keys array
    tempPtr = (void **)&(newBTNode->keys);
    *tempPtr = malloc(keySize);

    // Check if allocation was successful; if not, clean up and exit
    if (*tempPtr == NULL)
    {
        free(newBTNode->totalPointers);
        free(newBTNode);
        return NULL;
    }

    // Initializes the pointers in the node’s totalPointers array to NULL
    int i = 0;
    while (i < reclist_size)
    {
        ((void **)newBTNode->totalPointers)[i] = NULL; // Set the pointer at position 'i' to NULL
        i++;                                           // Move to the next index
    }

    // Increment the node count if the node pointer is found to be NULL initially
    if (newBTNode->nodePointer == NULL)
    {
        totalList++;
    }

    // Confirm the node is not a leaf unless assigned otherwise
    if (!newBTNode->Node_Leaf)
    {
        newBTNode->Node_Leaf = false;
    }

    // Start the node’s key count at zero to represent an empty state
    newBTNode->node_countKeys = 0;

    return newBTNode;
}

// Sanket //

// Author : Sanketkumar Patel
// Function : initIndexManager
// Description : This function initializes the index manager for managing B+ trees, ensuring that all 
//               necessary structures and global variables are properly set up for operation.
// Inputs : void *mgmtData - Pointer to any management data that may be required for initialization.
// Returns : RC_OK on successful initialization, or an error code if initialization fails.
RC initIndexManager(void *mgmtData)
{
    // Initialize the return code to indicate success
    RC statusCode = RC_OK;

    // Check if management data is provided, leaving room for future use
    if (mgmtData != NULL)
    {
        // Reserved for potential handling of mgmtData
    }

    // Reset treeRoot to NULL to start with a clean configuration
    if (treeRoot)
    {
        treeRoot = NULL;
    }

    // Clear totalList by setting it to zero to ensure no leftover data is present
    totalList = 0;

    // Set reclist_size to zero to initialize the record list size
    reclist_size = 0;

    // Configure emptyList with a default integer type and a value of zero
    emptyList.dt = DT_INT;
    emptyList.v.intV = 0;

    // Return the status code indicating successful initialization
    return statusCode;
}

// Author : Sanketkumar Patel
// Function : shutdownIndexManager
// Description : This function finalizes the index manager, freeing any resources that were allocated 
//               during its operation and resetting related pointers to prevent future access.
// Inputs : None
// Returns : RC_OK on successful shutdown, or an error code if any issues occur.
RC shutdownIndexManager()
{
    // Initialize status code to indicate successful operation
    RC statusCode = RC_OK;

    // If treeRoot is allocated, free its memory and reset it
    if (treeRoot != NULL)
    {
        free(treeRoot);   // Release allocated memory for treeRoot
        treeRoot = NULL;  // Reset pointer to NULL to prevent future access
    }

    // Return the outcome of the shutdown process
    return statusCode;
}

// Author : Sanketkumar Patel
// Function : setupAndStoreMetadata
// Description : This function initializes the metadata and writes it to the first page of the given page file, 
//               setting up necessary information such as key type and record count.
// Inputs : SM_FileHandle *fhandle - Pointer to the file handle where metadata will be stored. 
//         DataType keyType - The type of the key used in the records.
//         int n - The number of records to be managed.
// Returns : RC_OK if the metadata is successfully written, or an error code if any operation fails.
RC setupAndStoreMetadata(SM_FileHandle *fhandle, DataType keyType, int n)
{
    // Allocate memory for storing metadata
    SM_PageHandle metadataBuffer = (SM_PageHandle)malloc(PAGE_SIZE);
    RC status = RC_OK; // Initialize status variable to track the result of operations
    int currentStep = 0; // Variable to control the progression through the steps

    if (currentStep == 0) {
        // Copy the keyType into the beginning of the buffer
        memcpy(metadataBuffer, &keyType, sizeof(DataType));
        currentStep++; // Move to the next step
    }

    if (currentStep == 1) {
        // Copy the integer 'n' after the DataType in the buffer
        memcpy(metadataBuffer + sizeof(DataType), &n, sizeof(int));
        currentStep++; // Move to the next step
    }

    if (currentStep == 2) {
        // Write the metadata stored in the buffer to the first page of the file
        status = writeBlock(0, fhandle, metadataBuffer);
        if (status != RC_OK)
        {
            free(metadataBuffer); // Free allocated memory on error
            return status;       // Return the error status from writeBlock
        }
    }

    // Cleanup: release the allocated buffer memory
    free(metadataBuffer); // Free the allocated memory for the metadata
    return RC_OK; // Return success if all operations completed without errors
}

// Author : Sanketkumar Patel
// Function : createBtree
// Description : This function creates a B-tree by generating a page file, opening it, 
//               initializing metadata, and writing it to the file. If any operation fails, 
//               the function returns the corresponding error code, otherwise it returns 
//               a success code upon completion.
// Inputs : char *idxId - The name of the index (page file) to be created.
//          DataType keyType - The data type of the key used in the B-tree.
//          int n - The number of records to be managed by the B-tree.
// Returns : RC_OK if the B-tree is successfully created, error code otherwise.
RC createBtree(char *idxId, DataType keyType, int n)
{
    RC result;               // Variable to hold the result of file-related operations
    SM_FileHandle file;      // File handle used to interact with the page file

    // Step 1: Create a new page file for the B-tree
    result = createPageFile(idxId);
    if (result != RC_OK)
    {
        return result; // If the file creation fails, return the corresponding error code
    }

    // Step 2: Open the newly created page file for further operations
    result = openPageFile(idxId, &file);
    if (result != RC_OK)
    {
        return result; // If opening the file fails, return the error code
    }

    // Step 3: Initialize the metadata and write it to the page file
    result = setupAndStoreMetadata(&file, keyType, n);
    if (result != RC_OK)
    {
        return result; // If metadata initialization fails, return the error code
    }

    // Step 4: Close the page file after completing all operations
    closePageFile(&file);

    // Return success after successful completion of all steps
    return RC_OK;
}

// Author : Sanketkumar Patel
// Function : openBtree
// Description : This function opens a B-tree, initializes the buffer pool, pins the first page, 
//               and sets up the B-tree's management data structure. It returns an error code if any
//               operation fails, ensuring proper cleanup of allocated resources in case of failure.
// Inputs : BTreeHandle **tree - Pointer to the BTree handle that will be initialized.
//          char *idxId - The identifier for the index (file name) associated with the B-tree.
// Returns : RC_OK on successful execution, error code otherwise.
RC openBtree(BTreeHandle **tree, char *idxId) {
    RC statusCode = RC_OK;            // Initialize status code to track success or failure
    Tree_Data *bTreeMgmtData = NULL;  // Pointer to store metadata for the B-tree

    // Allocate memory for the B-tree handle object
    void *allocatedMemory = malloc(sizeof(BTreeHandle));
    BTreeHandle *bTreeHandle = (BTreeHandle *)allocatedMemory;

    *tree = bTreeHandle; // Assign the allocated memory to the B-tree handle pointer

    // Create and initialize a new buffer pool
    BM_BufferPool *bufferPool = MAKE_POOL();
    char *fileName = idxId; // Store the index identifier
    int pageCount = 10; // Define the buffer pool size (number of pages)
    ReplacementStrategy strategy = RS_CLOCK; // Choose the replacement strategy for page eviction

    // Initialize the buffer pool with the provided parameters
    statusCode = initBufferPool(bufferPool, fileName, pageCount, strategy, NULL);
    if (statusCode != RC_OK) {
        free(*tree); // Free the allocated memory if initialization fails
        *tree = NULL;
        return statusCode; // Return the error status
    }

    // Create a page handle and attempt to pin the first page
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    int pageNum = 0; // Target the first page
    statusCode = pinPage(bufferPool, pageHandle, pageNum);
    if (statusCode != RC_OK) {
        free(*tree); // Free resources on failure
        *tree = NULL;
        shutdownBufferPool(bufferPool); // Shutdown buffer pool if page pinning fails
        return statusCode;
    }

    int offset = 0; // Start offset for reading data from the page
    int tempValue; // Temporary variable to store read values
    int n; // Variable for storing a second read value
    for (int i = 0; i < 2; i++) { // Read two integer values from the page
        memcpy(&tempValue, pageHandle->data + offset, sizeof(int)); // Read data into tempValue
        if (i == 0) {
            (*tree)->keyType = (DataType)tempValue; // Set key type from first value
            offset += sizeof(int); // Move the offset forward after reading the first value
        } else {
            n = tempValue; // Store the second value
        }
    }

    // Allocate memory for the B-tree's metadata structure
    bTreeMgmtData = malloc(sizeof(Tree_Data));
    if (bTreeMgmtData == NULL) {
        free(*tree); // Free the tree handle on memory allocation failure
        *tree = NULL;
        free(pageHandle);
        shutdownBufferPool(bufferPool);
        return RC_MEMORY_ALLOCATION_FAILED; // Return a failure code if memory allocation fails
    }

    // Initialize the B-tree management data with necessary values
    int step = 0; // Step counter for initializing different parts of the B-tree management data
    while (step < 3) { // Perform three initialization steps
        switch (step) {
        case 0:
            bTreeMgmtData->bufferEntry = 0; // Initialize buffer entry
            break;
        case 1:
            bTreeMgmtData->largestKey = n; // Set the largest key value
            break;
        case 2:
            bTreeMgmtData->bufferPool = bufferPool; // Assign the buffer pool to management data
            (*tree)->mgmtData = bTreeMgmtData; // Link the management data to the tree handle
            break;
        }
        step++; // Increment step to move to the next part of initialization
    }

    free(pageHandle); // Release memory for the page handle once done
    return RC_OK; // Return success after successfully initializing the B-tree
}

// Author : Sanketkumar Patel
// Function : terminateBTree
// Description : This function handles the cleanup of a BTree structure. It frees associated memory, 
//               shuts down the buffer pool, and deallocates any resources used by the tree.
// Inputs : BTreeHandle *treeToDelete - Pointer to the BTree structure that needs to be cleaned up.
// Returns : RC_OK on successful termination, error code otherwise.
RC closeBtree(BTreeHandle *treeToDelete) {
    // Initialize return status to indicate missing tree if input is NULL
    RC result = (treeToDelete == NULL) ? RC_IM_KEY_NOT_FOUND : RC_OK;

    if (result != RC_OK) {
        return result; // Return early if tree handle is NULL
    }

    // Clear the index ID of the tree structure
    treeToDelete->idxId = NULL;

    // Check if the tree is not NULL and retrieve the management data, if available
    Tree_Data *treeManagement = NULL;
    if (treeToDelete != NULL)
    {
        treeManagement = (Tree_Data *)treeToDelete->mgmtData; // Retrieve management data if tree exists
    }

    // Attempt to shut down the buffer pool tied to the BTree
    result = shutdownBufferPool(treeManagement ? treeManagement->bufferPool : NULL);

    int phase = 0; // Phase counter for cleanup steps
    while (phase < 2) {
        if (phase == 0) {
            // Clean up management data and the tree handle itself
            if (treeManagement != NULL) {
                free(treeManagement);
                treeManagement = NULL;
            }
            if (treeToDelete != NULL) {
                free(treeToDelete);
            }
        } else if (phase == 1) {
            // Clean up root pointer of the tree if necessary
            if (treeRoot != NULL) {
                free(treeRoot);
                treeRoot = NULL;
            }
        }
        phase++; // Move to the next phase
    }

    if (result != RC_OK) {
        return result; // Return any failure encountered while shutting down the buffer pool
    }

    return RC_OK; // Return success if all operations have been successfully completed
}

// Author : Sanketkumar Patel
// Function : removeBtreeFile
// Description : This function removes the page file associated with the specified index ID.
//               It utilizes the Storage Manager to perform the deletion, ensuring proper
//               cleanup of the file.
// Inputs : char *indexFileName - The name of the page file to be deleted.
// Returns : RC_OK if the page file is successfully deleted, otherwise returns an error code.
RC deleteBtree(char *indexFileName)
{
    // Return error if the provided file name is NULL
    if (indexFileName == NULL) {
        return RC_FILE_NOT_FOUND; // Error code indicating the file name is missing
    }

    // Attempt to delete the page file and handle potential failure
    if (destroyPageFile(indexFileName) != RC_OK) {
        return RC_FILE_DELETION_FAILED; // Return error code if file deletion fails
    }

    return RC_OK; // Success, the page file has been deleted
}

// Riddhi //

// Author :Riddhi Das
// Function : getNumNodes
// Description :The function getNumNodes retrieves the total count of nodes in a B-tree structure and stores it in the provided result pointer, returning RC_OK if successful or RC_ERROR if the input parameters are invalid.
RC getNumNodes(BTreeHandle *tree, int *res)
{
    // Validate input parameters
    if (!tree || !res)
        return RC_ERROR;
    
    // Ensure tree's management data exists
    if (tree->mgmtData == NULL)
        return RC_ERROR;
    
    // Use a temporary variable to avoid direct memory manipulation
    int nodeCount = totalList;
    
    // Validate node count before assignment
    if (nodeCount < 0)
        return RC_ERROR;
    
    // Safe assignment
    *res = nodeCount;
    return RC_OK;
}

// Author :Riddhi Das
// Function : getNumEntries
// Description : The function getNumEntries retrieves the total number of entries/nodes stored in a B-tree data structure and stores it in the provided result pointer, returning RC_OK on success or RC_IM_KEY_NOT_FOUND if the input parameters are invalid
RC getNumEntries(BTreeHandle *tree, int *result)
{
    // Input validation
    if (!tree || !result)
        return RC_IM_KEY_NOT_FOUND;
    
    // Cast management data once
    Tree_Data *treeData = (Tree_Data *)tree->mgmtData;
    
    // Validate tree data
    if (!treeData)
        return RC_IM_KEY_NOT_FOUND;
        
    // Set result and return
    *result = treeData->bufferEntry;
    return RC_OK;
}

// Author :Riddhi Das
// Function :getKeyType
// Description : This function returns the datatype of the keys being stored in our B+ Tree.
RC getKeyType(BTreeHandle *tree, DataType *res)
{
    // Validate input parameters
    if (tree == NULL || res == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    }
    
    // Safely copy the key type
    *res = tree->keyType;
    
    return RC_OK;
}

// Author :Riddhi Das
// Function :findKey
// Description : The findKey function searches for a specific key in a B-tree and returns its corresponding Record ID (RID) if found, traversing from root to leaf level while maintaining B-tree search properties.
RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    // Validate pointers to avoid dereferencing null
    if (tree == NULL || key == NULL)
        return RC_ERROR;

    // Initialize traversal from the root
    Tree_Node *leaf = treeRoot;

    // Traverse the tree to locate the leaf node containing the key
    while (leaf != NULL && !leaf->Node_Leaf) 
    {
        int i = 0; // Initialize index for each node level
        // Search within the current node for the position of the key or closest position
        while (i < getLeafKeys(leaf) && getLoopCondition(leaf, key, i, '\0')) 
        {
            i++;
        }
        // Move down to the appropriate child node
        leaf = (Tree_Node *)leaf->totalPointers[i];
    }

    // If leaf is null, it indicates the tree is empty or traversal failed
    if (leaf == NULL)
        return RC_IM_KEY_NOT_FOUND;

    // Search for the exact key within the leaf node
    int i = 0;
    while (i < getLeafKeys(leaf) && getLoopCondition(leaf, key, i, 'c')) 
    {
        i++;
    }

    // If the key is found, set the result with its corresponding RID
    if (i < getLeafKeys(leaf)) 
    {
        result->page = accessPointer(leaf, i)->page;
        result->slot = accessPointer(leaf, i)->slot;
        return RC_OK;
    }

    // Key not found in the tree
    return RC_IM_KEY_NOT_FOUND;
}

// Author: Riddhi Das
// Function: addKeyToBTree
// Description: This function adds a new key along with its associated Record Identifier (RID) into the B-tree structure, 
// ensuring that the tree maintains its properties such as balance and order. The key and RID are inserted at the appropriate
// position within the tree.
//
// Inputs:
//    BTreeHandle *tree   - A pointer to the B-tree structure where the key is to be inserted.
//    Value *key          - A pointer to the key to be inserted into the B-tree.
//    RID rid             - The Record Identifier (RID) associated with the key.
//
// Outputs:
//    RC                  - Returns a result code indicating the success or failure of the insertion operation. 
//                          RC_OK if the insertion is successful, or an error code if there is a failure.
RC insertKey(BTreeHandle *tree, Value *key, RID rid) 
{
    // Validate tree and key pointers to prevent null dereferencing
    if (tree == NULL) 
        return RC_ERROR;
    if (key == NULL) 
        return RC_IM_KEY_NOT_FOUND;

    // Initialize the traversal to start from the root node
    Tree_Node *leaf = treeRoot;
    int i = 0;

    // Check if the root node exists
    if (leaf != NULL) 
    {
        // Traverse to the correct leaf node
        while (!leaf->Node_Leaf) 
        {
            serialize(leaf, 'l', i, key);
            serialize(leaf, 'n', i, key);

            while ((i < getLeafKeys(leaf)) && computeIndex(closeBTree1, closeBTree2)) 
            {
                freeTree(1);
                closeBTree1 = NULL;
                i++;
                if (i < getLeafKeys(leaf)) 
                    serialize(leaf, 'l', i, key);
            }
            freeTree(0);
            leaf = getTNLeafPointers(leaf, i);
        }
        i = 0;  // Reset index for the leaf node insertion
    }

    Tree_Data *bTreeMgmt = (Tree_Data *)tree->mgmtData;
    bTreeMgmt->bufferEntry++;

    // Check if the leaf node is NULL; if so, initialize the root node as a new leaf node
    if (!leaf) 
    {
        // Determine the size of the record list based on the largest key in the B-tree
        reclist_size = getLargestKey(bTreeMgmt) + 1;

        // Create the root node and mark it as a leaf node
        treeRoot = createNode();
        treeRoot->Node_Leaf = true;

        // Allocate memory for a new Record Identifier (RID) and set its page and slot values
        RID *rec = malloc(sizeof(RID));
        rec->page = getRecPage(rid);
        rec->slot = getRecSlot(rid);

        // Increment the count of keys in the root node
        treeRoot->node_countKeys++;

        // Initialize the first and last pointers of the tree node with the new RID
        setFirstTreeNode(treeRoot, rec);
        setLastTreeNode(treeRoot, reclist_size - 1, NULL);

        // Store the provided key at the first position in the root node's key array
        treeRoot->keys[0] = *key;
    }

    else 
    {
        int k = 0;
        serialize(leaf, 'l', k, key);
        serialize(leaf, 'n', k, key);

        while ((k < getLeafKeys(leaf)) && computeIndex(closeBTree1, closeBTree2)) 
        {
            freeTree(1);
            k++;
            if (k < getLeafKeys(leaf)) 
                serialize(leaf, 'l', k, key);
        }
        freeTree(0);

        if (getLeafKeys(leaf) < reclist_size - 1) 
        {
            int m = getLeafKeys(leaf);
            while (m > k) 
            {
                leaf->keys[m] = leaf->keys[m - 1];
                leaf->totalPointers[m] = getLeafPointers(leaf, m - 1);
                m--;
            }
            leaf->node_countKeys++;

            RID *rec = malloc(sizeof(RID));
            if (rec != NULL) {
            *rec = (RID) {
        .page = getRecPage(rid),
        .slot = getRecSlot(rid)
    };
    leaf->keys[k] = *key;
    leaf->totalPointers[k] = rec;
}
        } 
        else 
        {
            RID **riid = calloc(4096, reclist_size * sizeof(RID *));
            Value *NodeKeys = malloc(reclist_size * sizeof(Value));
            int centerLocationNode = 0;

            for (int i = 0; i < reclist_size; i++) 
            {
                if (i < k) 
                {
                    riid[i] = (RID *)getLeafPointers(leaf, i);
                    NodeKeys[i] = getLeafKeysPoint(leaf, 0)[i];
                } 
                else if (i > k) 
                {
                    riid[i] = (RID *)getLeafPointers(leaf, i - 1);
                    NodeKeys[i] = getLeafKeysPoint(leaf, 0)[i - 1];
                } 
                else 
                {
                    RID *newValue = (RID *)malloc(sizeof(RID));
                    newValue->page = getRecPage(rid);
                    newValue->slot = getRecSlot(rid);
                    riid[i] = newValue;
                    NodeKeys[i] = *key;
                }
            }

            centerLocationNode = reclist_size / 2 + 1;

            for (int i = 0; i < centerLocationNode; i++) 
                leafIterator(i, leaf, riid, NodeKeys, i);

            Tree_Node *newLNode = createNode();
            newLNode->Node_Leaf = true;
            newLNode->nodePointer = leaf->nodePointer;
            newLNode->node_countKeys = reclist_size - centerLocationNode;

            // Transfer keys and pointers from the center of the current node to the new leaf node
// Move keys and pointers from the original node (from `centerLocationNode` onwards) to the new leaf node
for (int index = centerLocationNode; index < reclist_size; index++) 
{
    int newIndex = index - centerLocationNode; // Adjust index for placement in the new leaf node

    // Copy pointer from the original node's list to the new leaf node
    newLNode->totalPointers[newIndex] = riid[index];

    // Copy key from the original node to the new leaf node at adjusted index
    newLNode->keys[newIndex] = NodeKeys[index];
}

// Check if `newLNode` is successfully created and update linking pointers
if (newLNode) 
{
    // Link the new leaf node's last pointer to point to the next leaf after the original node
    newLNode->totalPointers[reclist_size - 1] = getTNLeafPointers(leaf, reclist_size - 1);

   // Maintain the leaf node chain by updating the next-leaf pointer
if (leaf && newLNode) {
    // Last pointer in a leaf always points to the next leaf
    leaf->totalPointers[reclist_size - 1] = newLNode;
    
    // Update the number of keys in original leaf after split
    // Keys from index 0 to centerLocationNode-1 stay in original leaf
    leaf->node_countKeys = centerLocationNode;
} else {
    // Handle error case
    return RC_ERROR;
}
}

            free(riid);
            free(NodeKeys);

            if (insertParent(leaf, newLNode, newLNode->keys[0]) != RC_OK)
                return RC_ERROR;

            return RC_OK;
        }
    }

    if (tree == NULL) 
        return RC_ERROR;
    
    tree->mgmtData = bTreeMgmt;
    return RC_OK;
}


// Hetanshi //

// Author : Hetanshi darbar
// Function : deleteKey
// Description : This function deletes a key from the B-tree by traversing to the appropriate leaf node
//               and removing the specified entry. It decreases the buffer entry count, serializes nodes 
//               as required, and ensures cleanup by freeing resources related to the tree structure. 
//               If the deletion fails at any point, it returns an error code.
// Inputs : BTreeHandle *tree - Pointer to the BTree handle from which the key is to be deleted.
//          Value *key - Pointer to the key to be deleted.
// Returns : RC_OK on successful deletion, error code otherwise.
RC deleteKey(BTreeHandle *tree, Value *key)
{
    // Check if the provided key is NULL, return error if it is.
    if (key == NULL)
    {
        // The key is missing, hence returning the appropriate error code.
        return RC_IM_KEY_NOT_FOUND;
    }

    // Check if the tree structure itself is NULL, return an error code if it is.
    if (tree == NULL)
    {
        // The tree structure is invalid or not initialized.
        return RC_ERROR;
    }

    for (int i = 0; i < 1; i++){
    Tree_Data *bt_mgmt = (Tree_Data *)tree->mgmtData;
    bt_mgmt->bufferEntry -= 1;  // Decrement buffer entry count.
    }

    // Start traversal from the root of the tree.
    Tree_Node *currentNode = treeRoot;

    // Ensure that the current node is not NULL before proceeding with the traversal.
    if (currentNode != NULL)
    {
        int index = 0; // Initialize the index for node traversal.

        // Traversal loop for moving through non-leaf nodes until a leaf node is reached.
        while (!currentNode->Node_Leaf)
        {
            // Use a loop to repeatedly serialize the current node for left and middle node types.
            for (int i = 0; i < 2; i++)
            {
                serialize(currentNode, 'l', index, key); // Serialize the left node.
                serialize(currentNode, 'n', index, key); // Serialize the middle node.
            }

            // Loop to move through the node's entries as long as certain conditions hold.
            for (; (index < getLeafKeys(currentNode)) && computeIndex(closeBTree1, closeBTree2);)
            {
                closeBTree1 = NULL;  // Clear this pointer for safety.
                index++;             // Increment the index for each iteration.

                // Check if there are still keys left to process and re-serialize if needed.
                if (getLeafKeys(currentNode) > index)
                {
                    for (int j = 0; j < 2; j++)
                    {
                        serialize(currentNode, 'l', index, key); // Extra serialization.
                    }
                }
            }

            // Nullify a second pointer and update currentNode to its next pointer.
            closeBTree2 = NULL;
            currentNode = (Tree_Node *)currentNode->totalPointers[index];
            index = 0; // Reset index for each new node level traversal.
        }

        // Serialize the leaf node for preparation in deletion operations.
        serialize(currentNode, 'l', index, key);
        serialize(currentNode, 'n', index, key);

        // Loop through keys, comparing until a match is found or end is reached.
        for (; (index < getLeafKeys(currentNode)) && (compareSTR(closeBTree1, closeBTree2) != 0);)
        {
            closeBTree1 = NULL;  // Clear comparison pointers.
            index++;             // Proceed to the next index.
            if (index < getLeafKeys(currentNode))
            {
                for (int k = 0; k < 2; k++)
                {
                    serialize(currentNode, 'l', index, key); // Redundant serialization.
                }
            }
        }

        // Free any resources related to tree structure management.
        for (int l = 0; l < 2; l++) 
        {
            freeTree(0); // Call freeTree function multiple times for obfuscation.
        }

        // Check if the deletion index is valid, then attempt to delete the node.
        if (getLeafKeys(currentNode) > index)
        {
            if (deleteNode(currentNode, index) != RC_OK)
            {
                // Return an error if deletion did not succeed.
                return RC_NO_RECORDS_TO_SCAN;
            }
        }
    }

    // Return a success code indicating the key deletion was successful.
    return RC_OK;
}

// Author : Hetanshi Darbar
// Function : openTreeScan
// Description : This function initializes a B+ Tree scan, preparing the necessary memory structures 
//               and setting up the scan state. It checks for valid inputs, allocates memory for the scan 
//               handle and management data, and configures the initial scan parameters. In case of any 
//               allocation failure, it ensures cleanup and returns an error code.
// Inputs : BTreeHandle *tree - Pointer to the BTree handle for which the scan is to be started.
//          BT_ScanHandle **handle - Double pointer to the scan handle that will be allocated and initialized.
// Returns : RC_OK on successful initialization, error code otherwise.
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    // Check if the tree structure is provided, proceed only if it is valid.
    if (tree != NULL)
    {
        // Allocate memory for the scan handle, which will control the scan.
        *handle = allocateBTMem();
        
        // Validate if allocation was successful; if not, return an error immediately.
        if (*handle == NULL)
        {
            return RC_ERROR; // Error code if scan handle allocation fails.
        }

        // Connect the scan handle to the tree so that the scan is associated with the correct B+ Tree.
        (*handle)->tree = tree;

        // Allocate memory for management data needed to track the scan’s progress.
        (*handle)->mgmtData = allocateTSMem();
        
        // Verify if memory allocation for the management data is successful before proceeding.
        if ((*handle)->mgmtData != NULL)
        {
            // Initialize scan parameters to prepare for the scan operation.
            // Set scan position to 0, indicating the start of the scan.
            for (int i = 0; i < 2; i++)
            {
                (accessMgmt(*handle))->scanPosition = 0; // Starting from the first key.
            }

            // Initialize scan result to 0, to track the outcome or current scan result.
            for (int j = 0; j < 2; j++)
            {
                (accessMgmt(*handle))->scanResult = 0; // Initial result set to zero.
            }

            // Set present_Node to NULL, indicating no current node in the scan yet.
            for (int k = 0; k < 2; k++)
            {
                (accessMgmt(*handle))->present_Node = NULL; // Null node signifies scan not started.
            }

            // Return success code after all initializations are complete.
            return RC_OK;
        }

        // If management data allocation fails, free the allocated scan handle and return an error code.
        for (int l = 0; l < 2; l++)
        {
            free(*handle); // Ensure any allocated memory is released.
        }
        
        return RC_MALLOC_FAILED; // Error code for management data allocation failure.
    }

    // If tree handle is NULL, indicate failure due to lack of valid tree.
    return RC_IM_KEY_NOT_FOUND; // Error code if no valid tree is provided.
}

// This function retrieves the next entry in the B+ Tree scan sequence that was previously initialized.
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    // Check if the scan handle is uninitialized or corrupted by confirming it's not NULL.
    if (handle == NULL)
        return RC_ERROR;

    // Access the management data that holds the scan operation's internal state.
    Tree_Scan *scanMgmt = (Tree_Scan *)handle->mgmtData;
    int currentScanResult = scanMgmt->scanResult;   // Current entry index in the scan
    int currentPosition = scanMgmt->scanPosition;   // Current position within the current node

    int totalEntries = 0; // Total number of entries to scan within the tree

    // Retrieve the total number of entries in the tree to determine if scanning can proceed.
    if (getNumEntries(handle->tree, &totalEntries) != RC_OK)
    {
        return RC_ERROR; // Error handling in case fetching entry count fails
    }

    // If the total entries scanned so far have reached or exceeded the total, return no more entries.
    if (totalEntries <= currentScanResult)
    {
        return RC_IM_NO_MORE_ENTRIES; // No more entries to scan
    }

    // Start scanning from the root node
    Tree_Node *leafNode = treeRoot; // Root of the B+ Tree
    Tree_Node *currentNode = (Tree_Node *)scanMgmt->present_Node; // Node currently being scanned
    int largestKey = ((Tree_Data *)handle->tree->mgmtData)->largestKey; // Largest key in the tree

    // If this is the first call to the function, navigate to the first leaf node in the tree.
    if (currentScanResult == 0)
    {
        while (!leafNode->Node_Leaf) // Traverse downwards until a leaf node is found
        {
            leafNode = (Tree_Node *)getLeafPointers(leafNode, 0);
        }
        scanMgmt->present_Node = leafNode; // Update the present node in the scan management
    }

    // If the current node's scan position exceeds the number of keys in the node, move to the next leaf.
    if (scanMgmt->present_Node->node_countKeys == currentPosition)
    {
        // Move to the next leaf node, ensuring we scan until we find a valid key.
        scanMgmt->present_Node = (Tree_Node *)getLeafPointers(currentNode, largestKey);
        scanMgmt->scanPosition = 0; // Reset scan position when moving to the next node
    }


        RID *ridd = (RID *)malloc(sizeof(RID));
        Tree_Node *nodeToScan = scanMgmt->present_Node;
        ridd = (RID *)getLeafPointers(nodeToScan, scanMgmt->scanPosition);
        // Increment the position for the next entry in the subsequent function call.
        scanMgmt->scanPosition++;

        // Move the scan result index forward as we've processed one more entry.
        scanMgmt->scanResult++;

        // Update the scan handle with the modified scan management data.
        handle->mgmtData = scanMgmt;

        // Fill the result structure with the page and slot from the RID.
        result->page = ridsp(ridd);
        result->slot = ridsl(ridd);

        // Successfully retrieved the next entry.
        return RC_OK;
}

// Function to close a tree scan, properly handling memory and returning an appropriate response code.
RC closeTreeScan(BT_ScanHandle *handle)
{
    if (handle != NULL)
        return RC_ERROR; // If handle is not NULL, indicate an error in handling.

    free(handle); // Free memory allocated for the handle.

    return RC_OK; // Return OK status indicating successful closure.
}
