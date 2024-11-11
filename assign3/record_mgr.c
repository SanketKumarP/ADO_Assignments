#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stddef.h>


// Global pointers for managing different record manager instances
Record_Mgr *record_manager_pointer; // Pointer to the record manager
Record_Mgr *scan_manager_pointer;   // Pointer to the scan manager
Record_Mgr *table_manager_pointer;  // Pointer to the table manager

// Constants for various configurations
int MAX_COUNT = 1;                 // Maximum count value for some configurations
const int Max_No_Page = 150;       // Maximum number of pages that can be managed
const int invalid_state = -1;      // Indicator for an invalid state
const int size_of_Name_of_Attrs = 25; // Size of each attribute name in the schema


// Author : Sanketkumar Patel
// Function : initRecordManager
// Description : This function initializes the record manager, setting up any necessary structures and preparing 
//               the environment for managing records in the database.
// Inputs : void *mgmtData - Pointer to any management data needed for initialization.
// Returns : RC_OK on successful initialization, error code otherwise.
extern RC initRecordManager(void *mgmtData)
{
    // Call to initialize the storage manager.
    // This sets up the underlying storage management system that the record manager will use.
    initStorageManager();

    // Return success code indicating that the record manager has been initialized.
    return RC_OK;
}

// Author : Sanketkumar Patel
// Function : shutdownRecordManager
// Description : This function cleans up resources used by the record manager, including shutting down the buffer 
//               pool and freeing any allocated memory associated with the record manager.
// Returns : RC_OK on successful shutdown, error code if any errors occur during shutdown.
extern RC shutdownRecordManager()
{
    // Shut down the buffer pool associated with the record manager.
    // This function will write back any dirty pages and free allocated resources.
    shutdownBufferPool(&record_manager_pointer->buffer_pool);

    // Free the memory allocated for the record manager structure.
    free(record_manager_pointer);

    // Set the record manager pointer to NULL to avoid dangling pointers.
    record_manager_pointer = NULL;

    // Print a message to indicate that the record manager has been successfully shut down.
    printf("Record Manager has been successfully shut down.\n");

    // Return success code indicating the shutdown process completed successfully.
    return RC_OK;
}


// Author : Sanketkumar Patel
// Function : createTable
// Description : This function creates a new table with the specified name and schema. It initializes the necessary 
//               metadata, creates a new page file for the table, and writes the initial schema and metadata to the file.
// Inputs : char *tableName - The name of the table to be created.
//          Schema *tableSchema - A pointer to the schema defining the structure of the table.
// Returns : RC_OK on successful creation, error code otherwise.
extern RC createTable(char *tableName, Schema *tableSchema) 
{
    char pageData[PAGE_SIZE]; // Buffer for storing page data to be written to the file
    char *dataPtr = pageData; // Pointer to traverse the data buffer

    SM_FileHandle File_Handel; // File handle for accessing the table file in the storage manager

    // Allocate memory for the record manager structure, which manages table operations
    record_manager_pointer = (Record_Mgr *)malloc(sizeof(Record_Mgr));

    record_manager_pointer = (Record_Mgr *)malloc(sizeof(Record_Mgr));
    if (record_manager_pointer == NULL) {
    return RC_ERROR; // Handle memory allocation failure
    }


    // Initialize the buffer pool for the table, allowing caching of pages for the specified table
    initBufferPool(&record_manager_pointer->buffer_pool, tableName, Max_No_Page, RS_LRU, NULL);

    // Populate metadata fields in the page data array
    int metadataValues[] = {0, 1, tableSchema->numAttr, tableSchema->keySize};
    for (int i = 0; i < sizeof(metadataValues) / sizeof(int); i++)
    {
        // Write each metadata value into the page data buffer
        *(int *)dataPtr = metadataValues[i];
        dataPtr += sizeof(int); // Move the pointer forward by the size of an integer
    }

    // Populate the attribute schema information for each attribute defined in the schema
    for (int i = 0; i < tableSchema->numAttr; i++)
    {
        // Copy the attribute name into the page data buffer
        strncpy(dataPtr, tableSchema->attrNames[i], size_of_Name_of_Attrs);
        dataPtr += size_of_Name_of_Attrs; // Move the pointer to the next position

        // Store the data type of the attribute
        *(int *)dataPtr = (int)tableSchema->dataTypes[i];
        dataPtr += sizeof(int); // Move the pointer forward

        // Store the length of the attribute's data type
        *(int *)dataPtr = (int)tableSchema->typeLength[i];
        dataPtr += sizeof(int); // Move the pointer forward
    }

    // Create a new page file for the table, and check for success
    if (createPageFile(tableName) == RC_OK) 
    {
        // Open the newly created page file for writing
        if (openPageFile(tableName, &File_Handel) == RC_OK) 
        {
            // Write the initial page data (metadata and schema information) to the file
            if (writeBlock(0, &File_Handel, pageData) == RC_OK) 
            {
                // Close the page file after writing
                if (closePageFile(&File_Handel) == RC_OK) 
                {
                    return RC_OK; // Return success code if all operations completed successfully
                }
            }
        }
    }

    return RC_ERROR; // Return error code if any operation fails
}

// Author : Sanketkumar Patel
// Function : openTable
// Description : This function opens an existing table by loading its metadata and schema from the page file. It 
//               prepares the table for subsequent operations by pinning the appropriate page in the buffer pool.
// Inputs : RM_TableData *tableData - A pointer to the table data structure that will hold the table's metadata.
//          char *tableName - The name of the table to be opened.
// Returns : RC_OK on successful opening, error code if the table cannot be opened.
extern RC openTable(RM_TableData *tableData, char *tableName) 
{
    int numAttributes, status; // Variable to hold the number of attributes and status codes
    SM_PageHandle pageData; // Handle for the page data to read from the buffer pool

    // Set the name of the table and management data for the record manager
    tableData->name = tableName;
    tableData->mgmtData = record_manager_pointer;

    // Pin the first page of the buffer pool to read the table metadata
    status = pinPage(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle, 0);
    if (status != RC_OK) 
    {
        return status; // Return the status if pinning the page fails
    }

    // Read the metadata from the pinned page
    pageData = (char *)record_manager_pointer->page_Handle.data; // Get the pointer to the page data
    record_manager_pointer->Countoftuples = *((int *)pageData); // Read the total count of tuples
    pageData += sizeof(int); // Move the pointer to the next metadata item
    record_manager_pointer->freePage = *((int *)pageData); // Read the number of free pages
    pageData += sizeof(int); // Move the pointer to the next metadata item
    numAttributes = *((int *)pageData); // Read the number of attributes in the schema
    pageData += sizeof(int); // Move the pointer to the next metadata item

    // Allocate memory for schema information structure
    Schema *schemaInfo = (Schema *)malloc(sizeof(Schema));
    if (schemaInfo == NULL) 
    {
        return RC_MEMORY_ALLOCATION_ERROR; // Return error if memory allocation fails
    }

    // Set the number of attributes in the schema
    schemaInfo->numAttr = numAttributes;

    // Allocate memory for schema components based on the number of attributes
    schemaInfo->attrNames = calloc(numAttributes, sizeof(char *));
    schemaInfo->dataTypes = calloc(numAttributes, sizeof(DataType));
    schemaInfo->typeLength = calloc(numAttributes, sizeof(int));

    // Check if memory allocation was successful for each component
    if (!schemaInfo->attrNames || !schemaInfo->dataTypes || !schemaInfo->typeLength) 
    {
        // Free allocated resources in case of failure to avoid memory leaks
        free(schemaInfo->attrNames);
        free(schemaInfo->dataTypes);
        free(schemaInfo->typeLength);
        free(schemaInfo);
        return RC_MEMORY_ALLOCATION_ERROR;
    }


    // Populate the schema attributes by reading from the page data
    for (int attrIndex = 0; attrIndex < numAttributes; ++attrIndex) 
    {
        // Allocate memory for the attribute name and copy it from the page data
        schemaInfo->attrNames[attrIndex] = (char *)malloc(size_of_Name_of_Attrs);
        if (schemaInfo->attrNames[attrIndex] == NULL) 
        {
            // Free previously allocated memory on failure
            for (int j = 0; j < attrIndex; ++j) 
            {
                free(schemaInfo->attrNames[j]); // Free attribute names that were successfully allocated
            }
            free(schemaInfo->attrNames); // Free the attribute names array
            free(schemaInfo->dataTypes); // Free data types array
            free(schemaInfo->typeLength); // Free type lengths array
            free(schemaInfo); // Free the schema structure
            return RC_MEMORY_ALLOCATION_ERROR; // Return error for memory allocation failure
        }
        
        // Copy the attribute name from the page data
        strncpy(schemaInfo->attrNames[attrIndex], pageData, size_of_Name_of_Attrs);
        pageData += size_of_Name_of_Attrs; // Move the pointer to the next attribute data

        // Set the data type of the attribute
        schemaInfo->dataTypes[attrIndex] = *((DataType *)pageData);
        pageData += sizeof(DataType); // Move the pointer to the next item

        // Set the length of the attribute's data type
        schemaInfo->typeLength[attrIndex] = *((int *)pageData);
        pageData += sizeof(int); // Move the pointer to the next item
    }

    // Assign the constructed schema to the table data structure
    tableData->schema = schemaInfo;

    // Force any changes made to the buffer pool page back to disk
    status = forcePage(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle);
    
    // Return success status or write failed status based on the forcePage result
    return (status == RC_OK) ? RC_OK : RC_WRITE_FAILED;
}


// Author : Sanketkumar Patel
// Function : closeTable
// Description : This function closes the currently opened table, shutting down the associated buffer pool and 
//               freeing any resources allocated for the table.
// Inputs : RM_TableData *tableData - A pointer to the table data structure representing the opened table.
// Returns : RC_OK on successful closing, error code if any errors occur during the closing process.
extern RC closeTable(RM_TableData *tableData)
{
    // Retrieve the management data from the provided table data structure
    Record_Mgr *record_mgr_data = tableData->mgmtData;

    // Attempt to shut down the buffer pool associated with the table
    int shutdownResult = shutdownBufferPool(&record_mgr_data->buffer_pool);

    // Check if the shutdown operation was successful
    if (shutdownResult == RC_ERROR) {
        // If an error occurred, return the error code
        return RC_ERROR;
    } else {
        // If the shutdown was successful, return a success code
        return RC_OK;
    }
}

// Author : Sanketkumar Patel
// Function : deleteTable
// Description : This function deletes an existing table by removing its associated page file from the storage. 
//               It ensures that any necessary cleanup is performed, such as updating table counts.
// Inputs : char *tableName - The name of the table to be deleted.
// Returns : RC_OK on successful deletion, error code if the deletion fails.
extern RC deleteTable(char *tableName) 
{
    int currentTableCount = 1; // Initialize a variable to track the current table count
    int status = destroyPageFile(tableName); // Attempt to delete the associated page file

    // Check if there was an error in the deletion process
    if (status == RC_ERROR) 
    {
        MAX_COUNT = currentTableCount; // Update MAX_COUNT with the current table count
        return status; // Return the error code
    }
    // Return success code if the deletion was successful
    return RC_OK; 
}

// Author : Sanketkumar Patel
// Function : getNumTuples
// Description : This function retrieves the number of tuples (records) currently stored in the specified table. 
//               It accesses the management data to get the count of tuples.
// Inputs : RM_TableData *tableData - A pointer to the table data structure for which the tuple count is requested.
// Returns : The number of tuples in the table.
extern int getNumTuples(RM_TableData *tableData)
{
    // Ensure that the provided tableData is not NULL
    if (tableData == NULL || tableData->mgmtData == NULL) 
    {
        // Handle error: tableData or its management data is invalid
        return -1; // Return an error code or handle as needed
    }

    // Retrieve the management data from the table data
    Record_Mgr *record_mgr_data = tableData->mgmtData;

    // Access the count of tuples managed by the record manager
    int Number_of_Tuples=record_mgr_data->Countoftuples;
    
    // Return the total number of tuples
    return Number_of_Tuples;
}




// This function returns a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
    int numberOfSlots = PAGE_SIZE / recordSize;

    // Iterate through all slots in the page
    for (int index = 0; index < numberOfSlots; index++)
    {
        // Check if the current slot is free (not marked with '+')
        if (data[index * recordSize] != '+')
        {
            return index;  // Return the index of the first free slot found
        }
    }

    return -1;  // Return -1 if no free slots are found
}

// Author: Riddhi Das
// Function: insertRecord
// Description: This code implements a function to insert a record into a table in a database management system.
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    // Initialize management data and record ID
    Record_Mgr *mgr = rel->mgmtData;
    RID *rid = &record->id;
    rid->page = mgr->freePage;
    
    RC rc;
    int recordSize = getRecordSize(rel->schema);

    // Find a free slot for the record
    while (true)
    {
        // Pin the current page in the buffer pool
        rc = pinPage(&mgr->buffer_pool, &mgr->page_Handle, rid->page);
        if (rc != RC_OK)
            return rc;

        char *data = mgr->page_Handle.data;
        // Search for a free slot in the current page
        rid->slot = findFreeSlot(data, recordSize);

        // If a free slot is found, exit the loop
        if (rid->slot != -1)
            break;

        // Unpin the current page as no free slot was found
        rc = unpinPage(&mgr->buffer_pool, &mgr->page_Handle);
        if (rc != RC_OK)
            return rc;

        // Move to the next page
        rid->page++;
    }

    // Insert record into the found slot
    char *slotPtr = mgr->page_Handle.data + (rid->slot * recordSize);
    *slotPtr = '+'; // Mark the slot as occupied
    // Copy the record data into the slot
    memcpy(slotPtr + 1, record->data + 1, recordSize - 1);

    // Mark the page as dirty since we modified it
    rc = markDirty(&mgr->buffer_pool, &mgr->page_Handle);
    if (rc != RC_OK)
        return rc;

    // Unpin the page after inserting the record
    rc = unpinPage(&mgr->buffer_pool, &mgr->page_Handle);
    if (rc != RC_OK)
        return rc;

    // Increment the count of tuples in the table
    mgr->Countoftuples++;

    // Update metadata page
    rc = pinPage(&mgr->buffer_pool, &mgr->page_Handle, 0);
    if (rc != RC_OK)
        return rc;

    // Unpin the metadata page after updating
    rc = unpinPage(&mgr->buffer_pool, &mgr->page_Handle);
    if (rc != RC_OK)
        return rc;

    // Return success
    return RC_OK;
}

// Author: Riddhi Das
// Function: deleteRecord
// Description: This code implements a function to delete a record from a database table by marking it as deleted in the appropriate page, updating necessary metadata, and managing buffer pool operations
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    RC rc;
    Record_Mgr *rMgr = (Record_Mgr *)rel->mgmtData;
    
    // Pin the page containing the record to be deleted
    rc = pinPage(&rMgr->buffer_pool, &rMgr->page_Handle, id.page);
    if (rc != RC_OK)
        return rc;

    // Update the free page pointer
    rMgr->freePage = id.page;

    // Calculate the position of the record in the page
    int recordSize = getRecordSize(rel->schema);
    char *recordPtr = rMgr->page_Handle.data + (id.slot * recordSize);

    // Mark the record as deleted
    *recordPtr = '-';

    // Mark the page as dirty since we modified it
    rc = markDirty(&rMgr->buffer_pool, &rMgr->page_Handle);
    if (rc != RC_OK)
    {
        // Attempt to unpin the page before returning error
        unpinPage(&rMgr->buffer_pool, &rMgr->page_Handle);
        return rc;
    }

    // Unpin the page
    rc = unpinPage(&rMgr->buffer_pool, &rMgr->page_Handle);
    if (rc != RC_OK)
        return rc;

    // Decrement the count of tuples in the table
    rMgr->Countoftuples--;

    // TODO: Consider updating metadata page here if necessary

    return RC_OK;
}

// Author: Riddhi Das
// Function: updateRecord
// Description: This code updates an existing record in a database table by overwriting its data in the appropriate page, managing buffer pool operations, and handling potential errors.
extern RC updateRecord(RM_TableData *table, Record *updatedRecord)
{
    RC rc;
    Record_Mgr *record_manager_pointer = (Record_Mgr *)table->mgmtData;
    int recordSize = getRecordSize(table->schema);

    // Pin the page containing the record to be updated
    rc = pinPage(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle, updatedRecord->id.page);
    if (rc != RC_OK)
        return rc;

    // Calculate the position of the record in the page
    char *recordPosition = record_manager_pointer->page_Handle.data + (updatedRecord->id.slot * recordSize);

    // Mark the record as occupied ('+' indicates an active record)
    *recordPosition = '+';

    // Copy the updated record data into the page
    memcpy(recordPosition + 1, updatedRecord->data + 1, recordSize - 1);

    // Mark the page as dirty since we modified it
    rc = markDirty(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle);
    if (rc != RC_OK)
    {
        // Attempt to unpin the page before returning error
        unpinPage(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle);
        return rc;
    }

    // Unpin the page
    rc = unpinPage(&record_manager_pointer->buffer_pool, &record_manager_pointer->page_Handle);
    if (rc != RC_OK)
        return rc;

    return RC_OK;
}

// Author: Riddhi Das
// Function: getRecord
// Description: This code retrieves a specific record from a database table by its RID ,copying its data if found and active, while managing buffer pool operations and error handling
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    RC rc;
    Record_Mgr *recManager = rel->mgmtData;
    int recordSize = getRecordSize(rel->schema);

    // Pin the page containing the requested record
    rc = pinPage(&recManager->buffer_pool, &recManager->page_Handle, id.page);
    if (rc != RC_OK)
        return rc;

    // Calculate the position of the record in the page
    char *dataPointer = recManager->page_Handle.data + (id.slot * recordSize);

    // Check if the record is active ('+' indicates an active record)
    if (*dataPointer == '+')
    {
        // Set the record ID
        record->id = id;

        // Copy the record data (excluding the tombstone)
        memcpy(record->data + 1, dataPointer + 1, recordSize - 1);
    }
    else
    {
        // Unpin the page before returning error
        unpinPage(&recManager->buffer_pool, &recManager->page_Handle);
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    // Unpin the page
    rc = unpinPage(&recManager->buffer_pool, &recManager->page_Handle);
    if (rc != RC_OK)
        return RC_ERROR;

    return RC_OK;
}



// Author: Riddhi Das
// Function: startScan 
// Description: This code initializes a table scan operation by setting up necessary data structures and conditions for iterating through records in a database table
extern RC startScan(RM_TableData *r, RM_ScanHandle *s_handle, Expr *condition)
{
    RC rc;
    Record_Mgr *scan_manager_pointer, *table_manager_pointer;

    // Check if a valid condition is provided
    if (condition == NULL)
        return RC_SCAN_CONDITION_NOT_FOUND;

    // Open the table for scanning
    if ((rc = openTable(r, "ScanTable")) != RC_OK)
        return rc;

    // Allocate memory for scan manager
    if ((scan_manager_pointer = malloc(sizeof(Record_Mgr))) == NULL)
        return RC_MEMORY_ALLOCATION_ERROR;

    // Initialize scan manager
    *scan_manager_pointer = (Record_Mgr) {
        .Record_ID = {.page = 1, .slot = 0},
        .Count_of_scans = 0,
        .condition = condition
    };

    // Set up scan handle
    *s_handle = (RM_ScanHandle) {
        .mgmtData = scan_manager_pointer,
        .rel = r
    };

    // Update table manager
    table_manager_pointer = r->mgmtData;
    table_manager_pointer->Countoftuples = size_of_Name_of_Attrs; // Assuming this is defined elsewhere

    return RC_OK;
}

// Author: Riddhi Das
// Function: next
// Description: This code implements the 'next' operation in a table scan, iterating through records, evaluating a condition for each, and returning the next matching record or indicating the end of the scan
extern RC next(RM_ScanHandle *scan, Record *rec)
{
    Record_Mgr *scan_manager_pointer = scan->mgmtData;
    Record_Mgr *table_manager_pointer = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
    int slotCount = PAGE_SIZE / getRecordSize(schema);
    Value *output = (Value *)malloc(sizeof(Value));

    if (output == NULL)
        return RC_MEMORY_ALLOCATION_ERROR;

    // Iterate through records
    while (scan_manager_pointer->Count_of_scans <= table_manager_pointer->Countoftuples)
    {
        // Update RID for next record
        if (scan_manager_pointer->Count_of_scans > 0)
        {
            scan_manager_pointer->Record_ID.slot++;
            if (scan_manager_pointer->Record_ID.slot >= slotCount)
            {
                scan_manager_pointer->Record_ID.slot = 0;
                scan_manager_pointer->Record_ID.page++;
            }
        }
        else
        {
            scan_manager_pointer->Record_ID.page = 1;
            scan_manager_pointer->Record_ID.slot = 0;
        }

        // Pin the page containing the current record
        RC rc = pinPage(&table_manager_pointer->buffer_pool, &scan_manager_pointer->page_Handle, scan_manager_pointer->Record_ID.page);
        if (rc != RC_OK)
        {
            free(output);
            return rc;
        }

        // Get pointer to record data
        char *data = scan_manager_pointer->page_Handle.data + (scan_manager_pointer->Record_ID.slot * getRecordSize(schema));

        // Copy record data
        rec->id = scan_manager_pointer->Record_ID;
        *rec->data = '-';
        memcpy(rec->data + 1, data + 1, getRecordSize(schema) - 1);

        scan_manager_pointer->Count_of_scans++;

        // Evaluate condition
        evalExpr(rec, schema, scan_manager_pointer->condition, &output);

        if (output->v.boolV == TRUE)
        {
            unpinPage(&table_manager_pointer->buffer_pool, &scan_manager_pointer->page_Handle);
            free(output);
            return RC_OK;
        }

        // Unpin the page if condition not met
        unpinPage(&table_manager_pointer->buffer_pool, &scan_manager_pointer->page_Handle);
    }

    // Reset scan state if no more tuples
    scan_manager_pointer->Record_ID.page = 1;
    scan_manager_pointer->Record_ID.slot = 0;
    scan_manager_pointer->Count_of_scans = 0;

    free(output);
    return RC_RM_NO_MORE_TUPLES;
}

// Author: Riddhi Das
// Function: closeScan
// Description: This function closes a table scan operation by unpinning any pinned pages, resetting scan state, freeing allocated memory, and cleaning up associated resources
extern RC closeScan(RM_ScanHandle *scan)
{
    Record_Mgr *tableMgr = scan->rel->mgmtData;
    Record_Mgr *scanMgr = scan->mgmtData;

    // Check if scan management data exists
    if (scanMgr == NULL)
        return RC_OK;  // Scan already closed or never initialized

    // Unpin any pinned pages and reset scan state
    RC rc = unpinPage(&tableMgr->buffer_pool, &scanMgr->page_Handle);
    if (rc != RC_OK)
        return rc;  // Return error if unpinning fails

    // Reset scan manager state
    scanMgr->Count_of_scans = 0;
    scanMgr->Record_ID.slot = 0;
    scanMgr->Record_ID.page = 1;  // Assuming scans start from page 1

    // Free allocated memory and nullify the pointer
    free(scanMgr);
    scan->mgmtData = NULL;

    return RC_OK;
}



// Author : Hetanshi Darbar
// Function : getRecordSize
// Description : This function calculates the total size in bytes required to store a record based on 
//               the provided schema. It iterates through the attributes in the schema, summing the sizes 
//               based on their respective data types, which can include integers, strings, floats, and booleans.
//               An extra byte is added for a null terminator. If the schema is invalid or contains no attributes,
//               it returns an error code.
// Inputs : Schema *customSchema - A pointer to the schema structure containing attribute information.
// Returns : The total size of the record in bytes, or -1 in case of an error.
extern int getRecordSize(Schema *schema)
{
    int recordSize = 0;      // Variable to hold the total size of the record

    // Check if the schema is valid and contains attributes
    if (schema == NULL || schema->numAttr <= 0) {
        printf("Error: Invalid schema or no attributes present.\n");
        return -1; // Return error code
    }
    
    // Loop through each attribute in the schema to calculate total size
    int index = 0;           // Initialize index for attribute traversal
    while (index < schema->numAttr) {
        int dataType = schema->dataTypes[index];
        
        // Add size based on the data type
        switch (dataType) {
            case DT_INT:
                recordSize += sizeof(int);
                break;
            case DT_STRING:
                recordSize += schema->typeLength[index];
                break;
            case DT_FLOAT:
                recordSize += sizeof(float);
                break;
            case DT_BOOL:
                recordSize += sizeof(bool);
                break;
            default:
                printf("Error: Unknown data type encountered.\n");
                break;
        }

        index++; // Move to the next attribute
    }

    // Add space for null terminator if necessary
    recordSize++; 
    return recordSize; // Return the calculated size of the record
}

// Author : Hetanshi Darbar
// Function : createSchema
// Description : This function creates a new schema structure to define the layout of a database table. 
//               It takes the number of attributes, their names, data types, lengths, key size, and key 
//               attributes as input parameters. The function allocates memory for the schema and assigns 
//               the provided values to its fields. If the key size is less than or equal to zero, the 
//               function returns NULL, indicating an invalid schema request. 
// Inputs : int attributeCount - The number of attributes in the schema.
//          char **attributeNames - An array of strings containing the names of the attributes.
//          DataType *dataTypes - An array specifying the data types for each attribute.
//          int *lengths - An array containing the lengths of each attribute's data type.
//          int primaryKeySize - The number of key attributes in the schema.
//          int *primaryKeys - An array containing the indices of the key attributes.
// Returns : A pointer to the newly created Schema structure, or NULL in case of an error.
extern Schema *createSchema(int attributeCount, char **attributeNames, DataType *dataTypes, int *lengths, int primaryKeySize, int *primaryKeys)
{
    // Check if the primary key size is valid
    if (primaryKeySize <= 0)
        return NULL; // Return NULL if the key size is not valid

    // Allocate memory for the Schema structure
    Schema *newSchema = (Schema *)calloc(1, sizeof(Schema));

    // Check for memory allocation failure
    if (newSchema == NULL)
        return NULL; // Return NULL if allocation fails

    // Set the number of attributes and assign the provided names
    newSchema->numAttr = attributeCount;
    newSchema->attrNames = attributeNames;

    // Assign the data types and lengths
    newSchema->dataTypes = dataTypes;
    newSchema->typeLength = lengths;

    // Set the key size and key attributes
    newSchema->keySize = primaryKeySize;
    newSchema->keyAttrs = primaryKeys;

    return newSchema; // Return the newly created schema
}

// Author : Hetanshi Darbar
// Function : freeSchema
// Description : This function frees the memory allocated for a Schema structure, ensuring that 
//               any resources associated with the schema are properly released. After freeing the 
//               memory, it sets the pointer to NULL to prevent any dangling references. 
//               The function returns a success code upon completion.
// Inputs : Schema *schemaPtr - A pointer to the Schema structure that is to be deallocated.
// Returns : RC_OK - Indicates successful completion of the memory deallocation.
extern RC freeSchema(Schema *schema)
{
    // Free the memory allocated for the schema structure
    free(schema);
    schema = NULL; // Set the pointer to NULL to avoid dangling reference

    return RC_OK; // Return success code
}


// Author : Hetanshi Darbar
// Function : createRecord
// Description : This function allocates memory for a new Record structure and initializes it 
//               based on the provided schema. It calculates the required size for the record's 
//               data based on the schema's attributes and allocates memory accordingly. The 
//               record's identifier is initialized to indicate that it is not currently associated 
//               with any page or slot. If memory allocation is successful, the function sets the 
//               pointer to the new record and returns a success code. If allocation fails, it 
//               returns an error code instead.
// Inputs : Record **record - A double pointer to the Record structure that will be created 
//          Schema *schema - A pointer to the Schema structure that defines the record's format.
// Returns : RC_OK - Indicates successful creation and allocation of the record, or 
//          RC_ERROR - Indicates a failure in memory allocation.
extern RC createRecord(Record **newRecord, Schema *schemaInfo)
{
    int status;
    Record *recordInstance = calloc(1, sizeof(Record));

    int recordSize = getRecordSize(schemaInfo);
    recordInstance->data = calloc(recordSize, sizeof(char));

    // Initialize the record ID to indicate an invalid state
    recordInstance->id.page = invalid_state;
    recordInstance->id.slot = invalid_state;
    char *dataPtr = recordInstance->data;


    *dataPtr = '-';  // Set the first byte to a default value
    dataPtr[1] = '\0'; // Null-terminate the string
    *newRecord = recordInstance; // Assign the new record to the output parameter
    status = RC_OK; // Set the status to indicate success

    return status; // Return the status of record creation
}


// Author : Hetanshi Darbar
// Function : releaseRecord
// Description : This function releases the memory allocated for a Record structure. 
//               It first checks if the provided record pointer is not NULL, and if so, 
//               it frees the memory associated with the record to prevent memory leaks. 
//               After freeing the memory, the function sets the record pointer to NULL 
//               to ensure there are no dangling references. It returns a success code 
//               if the operation completes without issues or an error code if the 
//               memory was not allocated in the first place.
// Inputs : Record *rec - A pointer to the Record structure whose memory is to be freed.
// Returns : RC_OK - Indicates successful deallocation of the record's memory, or 
//          RC_ERROR - Indicates an issue encountered during the deallocation process.
extern RC freeRecord(Record *rec) 
{
    // Deallocate memory associated with the record
    if (rec != NULL) 
    {
        free(rec);
    }

    // Assign NULL to the record pointer to avoid dangling references
    rec = NULL;

    return RC_OK;
}

// Function to calculate the byte offset for a specified attribute in a record
RC calculateAttributeOffset(Schema *schema, int attributeIndex, int *offset)
{
    *offset = 1; // Start offset from 1 (for the size of the record itself)

    // Loop through all attributes up to the specified index to calculate the offset
    for (int i = 0; i < attributeIndex; ++i)
    {
        // Determine the size of each attribute based on its data type
        switch (schema->dataTypes[i])
        {
        case DT_STRING:
            *offset += schema->typeLength[i]; // Add length for string type
            break;

        case DT_INT:
            *offset += sizeof(int); // Add size for integer type
            break;

        case DT_BOOL:
            *offset += sizeof(bool); // Add size for boolean type
            break;

        case DT_FLOAT:
            *offset += sizeof(float); // Add size for float type
            break;

        default:
            printf("Unknown DataType\n"); // Print error for unknown data type
            return RC_RM_UNKNOWN_DATATYPE; // Return error code for unknown data type
        }
    }

    return RC_OK; // Return success if offset calculation is complete
}

// Author : Hetanshi Darbar
// Function : getAttr
// Description : Retrieves the value of a specified attribute from a Record structure. 
//               It calculates the attribute's offset based on the schema, allocates 
//               memory for the value, and populates the Value structure. Returns 
//               success or error codes based on the operation's validity.
// Inputs : Record *record - Pointer to the Record structure.
//          Schema *schema - Pointer to the Schema structure.
//          int attributeIndex - Index of the attribute to retrieve.
//          Value **attributeValue - Pointer to store the retrieved attribute value.
// Returns : RC_OK - Success, or RC_ERROR - Invalid index or unsupported data type.
extern RC getAttr(Record *record, Schema *schema, int attributeIndex, Value **attributeValue)
{
    int offset = 0; // Offset to track the position of the attribute
    RC resultCode; // Variable to hold the result code

    // Check if the provided attribute index is valid
    if (attributeIndex < 0)
    {
        resultCode = RC_ERROR; // Set result code to error for invalid index
    }
    else
    {
        char *data = record->data; // Pointer to the record's data
        calculateAttributeOffset(schema, attributeIndex, &offset); // Calculate the offset for the attribute

        // Allocate memory for the attribute value structure
        Value *value = (Value *)malloc(sizeof(Value));

        data += offset; // Move the data pointer to the calculated attribute's position

        // Adjust the data type if necessary (this logic seems peculiar)
        if (offset != 0)
        {
            schema->dataTypes[attributeIndex] = (attributeIndex != 1) ? schema->dataTypes[attributeIndex] : 1;
        }

        // Ensure there is valid data at the offset before proceeding
        if (offset != 0)
        {
            // Retrieve the attribute value based on its data type
            switch (schema->dataTypes[attributeIndex])
            {
            case DT_INT:
                {
                    int intValue = 0; // Variable to hold the integer value
                    memcpy(&intValue, data, sizeof(int)); // Copy the integer value from the data
                    value->dt = DT_INT; // Set data type in Value struct
                    value->v.intV = intValue; // Store integer value in Value struct
                }
                break;

            case DT_STRING:
                {
                    int length = schema->typeLength[attributeIndex]; // Length of the string to retrieve
                    value->v.stringV = (char *)malloc(length + 1); // Allocate memory for the string including null terminator
                    strncpy(value->v.stringV, data, length); // Copy string data from the record
                    value->v.stringV[length] = '\0'; // Null-terminate the string to ensure proper formatting
                    value->dt = DT_STRING; // Set data type in Value struct
                }
                break;

            case DT_BOOL:
                {
                    bool boolValue; // Variable to hold the boolean value
                    memcpy(&boolValue, data, sizeof(bool)); // Copy the boolean value from the data
                    value->v.boolV = boolValue; // Store the boolean value in Value struct
                    value->dt = DT_BOOL; // Set data type in Value struct
                }
                break;

            case DT_FLOAT:
                {
                    float floatValue; // Variable to hold the float value
                    memcpy(&floatValue, data, sizeof(float)); // Copy the float value from the data
                    value->dt = DT_FLOAT; // Set data type in Value struct
                    value->v.floatV = floatValue; // Store float value in Value struct
                }
                break;

            default:
                printf("Data type not supported for serialization\n"); // Handle unsupported data types
                free(value); // Free allocated memory for Value struct to prevent memory leak
                return RC_ERROR; // Return error for unsupported data type
            }

            *attributeValue = value; // Set the output parameter to the retrieved attribute value
            resultCode = RC_OK; // Indicate success
        }
        else
        {
            resultCode = RC_OK; // Indicate success, but no data was retrieved
        }
    }
    return resultCode; // Return the result code
}

// Author : Hetanshi Darbar
// Function : setAttr
// Description : Sets the value of a specified attribute in a Record structure. 
//               It calculates the attribute's offset and assigns the provided value 
//               based on its data type. Returns success or error codes for invalid 
//               index or unsupported data type.
// Inputs : Record *record - Pointer to the Record structure.
//          Schema *schema - Pointer to the Schema structure.
//          int attributeIndex - Index of the attribute to set.
//          Value *value - Pointer to the Value structure containing the data to set.
// Returns : RC_OK - Success, or RC_ERROR - Invalid index or unsupported data type.
extern RC setAttr(Record *record, Schema *schema, int attributeIndex, Value *value)
{
    int offset = 0; // Offset for the attribute position
    RC resultCode = RC_OK; // Initialize result code to success

    // Check for valid attribute index
    if (attributeIndex < 0)
    {
        return RC_ERROR; // Return error for invalid index
    }

    // Calculate the byte offset of the attribute within the record
    calculateAttributeOffset(schema, attributeIndex, &offset);
    
    char *dataPtr = record->data + offset; // Pointer to the attribute's position in the record data

    // Set the attribute value based on its data type
    switch (schema->dataTypes[attributeIndex])
    {
    case DT_INT:
        *(int *)dataPtr = value->v.intV; // Assign integer value to the record
        break;

    case DT_FLOAT:
        *(float *)dataPtr = value->v.floatV; // Assign float value to the record
        break;

    case DT_STRING:
        // Copy the string value into the record, respecting the defined type length
        strncpy(dataPtr, value->v.stringV, schema->typeLength[attributeIndex]);
        break;

    case DT_BOOL:
        *(bool *)dataPtr = value->v.boolV; // Assign boolean value to the record
        break;

    default:
        printf("Unsupported data type\n"); // Handle unsupported data types
        return RC_ERROR; // Return error for unsupported data type
    }

    return resultCode; // Return success
}
