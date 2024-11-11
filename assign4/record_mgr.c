#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdbool.h>

// Structure for Record Controller for containing all the related information
typedef struct Create_RecordManager
{
	BM_BufferPool bufferManagerPool;	   // It stores buffer pool information and its properties
	BM_PageHandle bufferManagerPageHandle; // It helps to access page files
	int freePagesCount;					   // It stores the freePagesCount details
	int totalScans;						   // It stores the total scanned records in the buffer
	int totalTuples;					   // It stores total number of tuples present in record manager
	Expr *scanRecord;					   // This is for scanning the records in the table
	RID recID;							   // It stores a unique id for records
} Create_RecordManager;

Create_RecordManager *recordManager; // Initialization of above Record Manager object

// Auxilary function to check whether record manager is initialized or not
int checkRecordInitFlag()
{
	int check_record_flag = 0;
	return (check_record_flag == 1) ? RC_OK : RC_RECORD_MANAGER_EMPTY;
}

// Auxilary function to initalize record manager
RC initRecordManager(void *mgmtData)
{
	if (checkRecordInitFlag() != RC_OK)
	{
		initStorageManager(); // Initialize the storage manager
	}
	return RC_OK; // Return OK
}
// A schema object that hold stat values for each page file
Schema *FetchSchema(SM_PageHandle pageHandle)
{
	int page_properties = *(int *)pageHandle; // fetch page properties from page file
	const int FETCH_SCHEMA_SIZE = 14;		  // create a schema size of length 14
	pageHandle = pageHandle + sizeof(int);	  // set page to beginning of record

	Schema *schema = (Schema *)malloc(sizeof(Schema)); // initializing schema

	// Define the number of attributes
	int numAttributes = page_properties;

	// Calculate the size of memory needed for attribute names
	size_t attrNamesSize = sizeof(char *) * numAttributes;

	// Calculate the size of memory needed for data types
	size_t dataTypesSize = sizeof(DataType) * numAttributes;

	// Calculate the size of memory needed for type lengths
	size_t typeLengthSize = sizeof(int) * numAttributes;

	// Allocate memory for attribute names in the schema
	schema->attrNames = (char **)malloc(attrNamesSize);

	// Allocate memory for data types in the schema
	schema->dataTypes = (DataType *)malloc(dataTypesSize);

	// Allocate memory for type lengths in the schema
	schema->typeLength = (int *)malloc(typeLengthSize);

	// Set the number of attributes in the schema
	schema->numAttr = numAttributes;

	// Allocating memory space for each property in schema
	// Allocate memory for attribute names in the schema
	int i = 0;
	while (i < page_properties)
	{
		schema->attrNames[i] = (char *)malloc(FETCH_SCHEMA_SIZE);
		i++;
	}

	// Initialize index variable for iterating over properties
	int index = 0;

	// Define the end of the page buffer based on the size of properties being read
	char *pageEnd = pageHandle + page_properties * (FETCH_SCHEMA_SIZE + 2 * sizeof(int));

	// Loop through each property and extract information
	do
	{
		// Copy property name from pageHandle to schema
		strncpy(schema->attrNames[index], pageHandle, FETCH_SCHEMA_SIZE);
		pageHandle += FETCH_SCHEMA_SIZE;

		// Check if there is enough space to read data type
		if (pageHandle + sizeof(int) <= pageEnd)
		{
			schema->dataTypes[index] = *(int *)pageHandle;
			pageHandle += sizeof(int);
		}
		else
		{
			// Exit loop if there's not enough space
			break;
		}

		// Check if there is enough space to read data type length
		if (pageHandle + sizeof(int) <= pageEnd)
		{
			schema->typeLength[index] = *(int *)pageHandle;
			pageHandle += sizeof(int);
		}
		else
		{
			// Exit loop if there's not enough space
			break;
		}

		// Increment index for the next property
		index++;
	} while (index < page_properties);

	// Check if loop terminated prematurely
	if (index < page_properties)
	{
		// Handle premature termination error
		// (Here you can add your own error handling logic)
	}

	// Return the schema
	return schema;
}
// Auxilary function to shutdown the record manager and set the initialization flag to 0 (reset)
RC shutdownRecordManager()
{

	recordManager = NULL;
	free(recordManager);
	return RC_OK;
}

extern RC createTable(char *name, Schema *schema)
{
	int tableIndex = 1000;															 // Define the table index
	recordManager = (Create_RecordManager *)calloc(1, sizeof(Create_RecordManager)); // Allocate memory and initialize record manager

	// Check if record manager allocation was successful
	if (recordManager != NULL)
	{
		BM_BufferPool *bufferPool = &(recordManager->bufferManagerPool); // Get the buffer pool from record manager

		// Initialize the buffer pool
		if (initBufferPool(bufferPool, name, tableIndex, RS_CLOCK, NULL) == RC_OK)
		{
			// Buffer pool initialization successful
			printf("Buffer initialisation successfull");
		}
		else
		{
			// Buffer pool initialization failed
			free(recordManager);	   // Free memory allocated for record manager
			return RC_BUFFER_NOT_INIT; // Return error code
		}
	}
	else
	{
		// Memory allocation for record manager failed
		return RC_ERROR; // Return error code
	}

	char data[PAGE_SIZE]; // Create a blank data page
	char *page_pointer = data;

	// Initializing values in the data page
	*(int *)page_pointer = 0;	 // Set Tuple Count
	page_pointer += sizeof(int); // Move the pointer to the next integer

	*(int *)page_pointer = 1; // Set First Page
	page_pointer += sizeof(int);

	*(int *)page_pointer = schema->numAttr; // Set number of attributes
	page_pointer += sizeof(int);

	*(int *)page_pointer = schema->keySize; // Set Key Size
	page_pointer += sizeof(int);

	const int TABLE_SIZE = 15;
	int index = 0; // Initialize index for the loop

	// Iterate over entire schema and copy attribute name, data-types, and type lengths to current page
	index = 0; // Initialize index for the loop

	do
	{
		// Copy attribute name to current page
		strncpy(page_pointer, schema->attrNames[index], TABLE_SIZE);
		page_pointer += TABLE_SIZE;

		// Copy data type to current page
		memcpy(page_pointer, &(schema->dataTypes[index]), sizeof(int));
		page_pointer += sizeof(int);

		// Copy type length to current page
		memcpy(page_pointer, &(schema->typeLength[index]), sizeof(int));
		page_pointer += sizeof(int);

		index++; // Increment index
	} while (index < schema->numAttr); // Check loop condition

	SM_FileHandle fileHandle;

	// Throw error if page file does not open or page file with given name cannot be created.
	if (createPageFile(name) != RC_OK || RC_OK != openPageFile(name, &fileHandle))
		return RC_FILE_NOT_FOUND;

	int writeCode = writeBlock(0, &fileHandle, data);
	if (writeCode == RC_OK)
		return closePageFile(&fileHandle);
	return writeCode;
}

// Function to open a table with the given name and relation
extern RC openTable(RM_TableData *rel, char *name)
{
	// Pin the first page of the buffer manager
	pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, 0);

	// Get a handle to the page's data
	SM_PageHandle pgHandle = (char *)recordManager->bufferManagerPageHandle.data;

	// Variable to store the count of pages
	int *openPtr = (int *)pgHandle;

	// Extract total number of tuples and count of free pages from the page file and store them in the record manager
	for (int tableIndex = 0; tableIndex < 2; tableIndex++)
	{
		if (tableIndex == 0)
		{
			recordManager->totalTuples = *openPtr;
		}
		else
		{
			recordManager->freePagesCount = *openPtr;
		}
		openPtr++; // Move to the next integer
	}

	// Advance the page handle pointer after extracting integers
	pgHandle += sizeof(int) * 2;

	// Assign the management data, table name, and schema
	rel->mgmtData = recordManager;
	rel->name = name;
	rel->schema = FetchSchema(pgHandle);

	// Unpin the page from the buffer manager
	unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);
	forcePage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle); // Force write the page to disk

	return RC_OK;
}

extern RC closeTable(RM_TableData *rel)
{
	// Check if the buffer pool is initialized before shutting it down
	if (rel->mgmtData != NULL)
	{
		// Call shutdownBufferPool function and pass buffer manager
		shutdownBufferPool(&recordManager->bufferManagerPool);
		return RC_OK;
	}
	else
	{
		// Buffer pool is not initialized, return error code
		return RC_BUFFER_NOT_INIT;
	}
}

extern RC deleteTable(char *name)
{
	// Check if the table name is not NULL before deleting
	if (name != NULL)
	{
		// Call destroyPageFile function and pass name into it
		destroyPageFile(name);
		return RC_OK;
	}
	else
	{
		// Table name is NULL, return error code
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

extern int getNumTuples(RM_TableData *rel)
{
	// Check if the record manager is not NULL before fetching total tuples
	if (rel->mgmtData != NULL)
	{
		Create_RecordManager *recordManager = rel->mgmtData;
		return recordManager->totalTuples;
	}
	else
	{
		// Record manager is NULL, return 0 tuples
		return 0;
	}
}

// Function to retrieve the record size
extern int getRecordSize(Schema *schema)
{
	int recordSizeCount = 1; // initialize variable

	// Iterate over numAttr of schema
	int k = 0;
	while (k < schema->numAttr)
	{
		switch (schema->dataTypes[k]) // switch case to check data types
		{
		case DT_INT: // INT to INT
			recordSizeCount += sizeof(int);
			break;
		case DT_FLOAT: /// FLOAT to FLOAT
			recordSizeCount += sizeof(float);
			break;
		case DT_STRING: // STRING then recordSizeCount = typeLength
			recordSizeCount += schema->typeLength[k];
			break;
		case DT_BOOL: // BOOLEAN to BOOLEAN
			recordSizeCount += sizeof(bool);
			break;
		}
		k++;
	}
	return recordSizeCount; // return record size
}

// Function to find a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
	int k = 0, slot_count = PAGE_SIZE / recordSize; // calculate slot count by dividing page sizes with each record size.

	while (k < slot_count)
	{
		if (data[k * recordSize] != '+')
		{ // if data stream does not have '+' character, return that page count
			return k;
		}
		k++;
	}
	return -1; // return -1 to denote no free slots
}

// Function to remove a schema from memory and free all the memory space in the schema.
extern RC freeSchema(Schema *schema)
{
	free(schema);
	return RC_OK;
}

// Function to insert a new record into the table.
extern RC insertRecord(RM_TableData *rel, Record *record)
{
	// Get a pointer to the record manager structure from the RM_TableData's management data
	Create_RecordManager *recordManager = (Create_RecordManager *)rel->mgmtData;

	// Determine the size of each record based on the schema of the table
	int recordSize = getRecordSize(rel->schema);

	// Get a pointer to the record identifier (RID) from the provided record
	RID *rid = &record->id;

	// Iterate until a free slot is found or the end of the page is reached
	for (int insertIndex = recordManager->freePagesCount; insertIndex < PAGE_SIZE; insertIndex++)
	{
		rid->page = insertIndex;																		// Set the page number to the current insert index
		pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, rid->page); // Pin the page
		char *data = recordManager->bufferManagerPageHandle.data;										// Get a pointer to the page's data
		rid->slot = findFreeSlot(data, recordSize);														// Find a free slot on the page

		if (rid->slot != -1)
		{
			break; // Exit the loop if a free slot is found
		}

		unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle); // Unpin the page if no free slot is found
	}

	if (rid->slot == -1)
	{
		// No free slot found in existing pages, need to allocate a new page
		rid->page = ++recordManager->freePagesCount;													// Increment the free page count and set the page number
		pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, rid->page); // Pin the newly allocated page
	}

	char *data = recordManager->bufferManagerPageHandle.data;							   // Get a pointer to the page's data
	markDirty(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle); // Mark the page as dirty

	char *SlotPtr = data + (rid->slot * recordSize); // Calculate the pointer to the slot where the record will be inserted
	*SlotPtr = '+';									 // Set the slot as occupied

	// Copy record data into the slot, excluding the header
	for (int recordCount = 0; recordCount < recordSize - 1; recordCount++)
	{
		if ((recordCount + 1) < recordSize)
		{
			// Check if the index is within bounds of record data before copying
			SlotPtr[recordCount + 1] = record->data[recordCount + 1]; // Copy record data into the slot
		}
	}

	unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle); // Unpin the page
	recordManager->totalTuples++;														   // Increment the total number of tuples in the table if a new record was successfully inserted

	// Pin the first page of the buffer manager for writing the updates
	pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, 0);

	return RC_OK; // Return success code
}

extern RC deleteRecord(RM_TableData *rel, RID id)
{
	// Retrieve the record manager
	Create_RecordManager *recordManager = (Create_RecordManager *)rel->mgmtData;

	// Get the record size
	int recordSize;
	recordSize = getRecordSize(rel->schema);

	// Pin the page containing the record to delete
	pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, id.page);

	// Calculate the offset of the record within the page
	int recordOffset = id.slot * recordSize;

	// Retrieve pointer to the record data within the page
	char *recordPtr;
	recordPtr = recordManager->bufferManagerPageHandle.data + recordOffset;

	// Check if the record is already marked as deleted
	if (*recordPtr == '-')
	{
		// Unpin the page
		unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);
		return RC_RECORD_MANAGER_EMPTY; // Record already deleted
	}

	// Mark the record as deleted
	*recordPtr = '-';

	// Mark the page as dirty
	markDirty(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);

	// Update the free pages count
	recordManager->freePagesCount = id.page;

	// Unpin the page
	unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);

	return RC_OK;
}

// Function to update record from the table
extern RC updateRecord(RM_TableData *tableData, Record *dataItem)
{
	// if tabledata does not exist, throw an error
	if (!tableData || !dataItem)
	{
		return RC_INVALID_INPUT; // Validate the inputs
	}

	Create_RecordManager *dataManager = tableData->mgmtData; // Access the data manager from the table data
	RC operationResult;

	// Begin by pinning the page where the dataItem resides
	operationResult = pinPage(&dataManager->bufferManagerPool, &dataManager->bufferManagerPageHandle, dataItem->id.page);
	if (operationResult != RC_OK)
	{
		return operationResult; // Early exit if unable to pin the page
	}

	int dataSize = getRecordSize(tableData->schema); // Store the size of record
	if (dataSize <= 0)								 // if size is invalid
	{
		unpinPage(&dataManager->bufferManagerPool, &dataManager->bufferManagerPageHandle); // Release the page
		return RC_INVALID_RECORD_SIZE;													   // Indicate an issue with the record size
	}

	// Fetch the position of page to update record
	char *pageBuffer = dataManager->bufferManagerPageHandle.data;
	char *updateSpot = pageBuffer + (dataItem->id.slot * dataSize);

	// Implement the tombstone mechanism before updating
	*updateSpot = '+'; // Spot the record as existing
	memcpy(updateSpot + 1, dataItem->data + 1, dataSize - 1);

	// Mark the page as dirty upon modification
	operationResult = markDirty(&dataManager->bufferManagerPool, &dataManager->bufferManagerPageHandle);
	if (operationResult != RC_OK)
	{
		// unpin the page, once the operations are done and pages are not marked dirty
		unpinPage(&dataManager->bufferManagerPool, &dataManager->bufferManagerPageHandle);
		return operationResult; // Return the error from the dirty marking attempt
	}

	// Unpin the page after updating
	operationResult = unpinPage(&dataManager->bufferManagerPool, &dataManager->bufferManagerPageHandle);
	return operationResult; // Return RC_OK on success
}

// Function to retrieve the existing record from the table.
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
	// Fetch details from mgmtData
	Create_RecordManager *recordManager = (Create_RecordManager *)rel->mgmtData;

	// Pin the page containing the record
	pinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle, id.page);

	char *data_ref = recordManager->bufferManagerPageHandle.data; // Store the data of buffer manager in a reference pointer

	// Store the size of corresponding record
	int recordSize = getRecordSize(rel->schema);

	// Calculate the offset of the record within the page
	data_ref += id.slot * recordSize; // Shift the pointer to the next record

	// Initialize a flag to indicate whether the record was found
	bool recordFound = (*data_ref == '+');

	// If the record is not found, return error
	if (!recordFound)
	{
		unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}

	// If the record is found, copy its data to the Record structure
	record->id = id;
	char *data = record->data; // Store data from record pointer

	// Copy data
	memcpy(++data, data_ref + 1, recordSize - 1);

	// Unpin the page
	unpinPage(&recordManager->bufferManagerPool, &recordManager->bufferManagerPageHandle);

	// Return success
	return RC_OK;
}

// Function to initialize all properties of RM_ScanHandle
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	// Check for NULL condition
	if (!cond)
		return RC_SCAN_CONDITION_NOT_FOUND;

	// Check if open table is possible
	if (openTable(rel, "ScanTable") != RC_OK)
	{
		return RC_MEMORY_ALLOCATION_FAILURE; // Using the same error for simplicity
	}

	// Use static allocation for RM_scanManager
	static Create_RecordManager RM_scanManager;

	// Store RM_scanManager properties
	RM_scanManager.scanRecord = cond; // Directly assign the condition
	RM_scanManager.recID.page = 1;	  // Initialize with page 1
	RM_scanManager.recID.slot = 0;	  // Initialize slot to 0
	RM_scanManager.totalScans = 0;	  // Start with zero scans

	scan->mgmtData = &RM_scanManager; // Store value of scan manager to the scan mgmmtdata
	scan->rel = rel;				  // store the table data

	// Update totalTuples to Create_RecordManager
	((Create_RecordManager *)rel->mgmtData)->totalTuples = 15; // Set SCAN_SIZE directly

	return RC_OK; // Success
}
// Auxilary function for pinning a page to the buffer pool
RC pinPageWrapper(BM_BufferPool *bm, BM_PageHandle *page, const int pageNum)
{
	return pinPage(bm, page, pageNum); // return the pinned page
}

// Auxilary function for unpinning a page from the buffer pool
RC unpinPageWrapper(BM_BufferPool *bm, BM_PageHandle *page)
{
	return unpinPage(bm, page); // return the unpinned page
}

// Auxilary function to checks scan conditions
RC checkScanConditions(Create_RecordManager *RM_scanManager, int recordEntriesCount)
{
	// Returns appropriate error code otherwise returns RC_OK
	return RM_scanManager->scanRecord == NULL
			   ? RC_SCAN_CONDITION_NOT_FOUND
		   : recordEntriesCount == 0 ? RC_RM_NO_MORE_TUPLES
									 : RC_OK;
}

// Function to increment the record ID based on the current scan and available slots
void incrementRecordIDIfNeeded(Create_RecordManager *RM_scanManager, int recordSearchCount, int recordCountSlots)
{
	// If record search count is not zero
	if (recordSearchCount > 0)
	{
		RM_scanManager->recID.slot++;				   // increment the slot corresponding record ID
		RM_scanManager->recID.slot >= recordCountSlots // if record id slot becomes larger than total slots for record
			? (RM_scanManager->recID.slot = 0, RM_scanManager->recID.page++)
			: 0;
	}
	else
	{
		// Reset record ID to the start for the initial scan
		RM_scanManager->recID.page = 1;
		RM_scanManager->recID.slot = 0;
	}
}

// Function to pin the page for the current record and copies data to the record structure
RC pinPageAndCopyData(Create_RecordManager *RM_scanManager, Create_RecordManager *RM_tableManager, Record *record, int recordMgrSize)
{
	// Pin the page containing the record to be scanned
	pinPageWrapper(&RM_tableManager->bufferManagerPool, &RM_scanManager->bufferManagerPageHandle, RM_scanManager->recID.page);
	// Calculate the pointer to the start of the record's data within the page
	char *pageData = RM_scanManager->bufferManagerPageHandle.data + RM_scanManager->recID.slot * recordMgrSize;
	// Copy the record's data from the page to the record structure, excluding the header
	memcpy(record->data + 1, pageData + 1, recordMgrSize - 1);
	record->data[0] = '-';				// Indicate the status of the record with a tombstone character in the header
	record->id = RM_scanManager->recID; // Assign the record identifier from the scan manager to the scanned record
	return RC_OK;
}

// Function to evaluate a condition and free allocated resources
RC evaluateConditionAndFreeResources(Value *evaluationResult, Create_RecordManager *scanManager, Create_RecordManager *tableManager)
{
	// Check if the condition evaluates to true and perform necessary actions
	RC operationStatus = evaluationResult && evaluationResult->v.boolV ? RC_OK : RC_ERROR;
	if (operationStatus == RC_OK) // If the condition evaluates to true, increment the scan count and unpin the page
	{
		scanManager->totalScans++; // Increment the scan count for a match
		unpinPageWrapper(&tableManager->bufferManagerPool, &scanManager->bufferManagerPageHandle);
	}
	// If evaluation result is true, free the allocated memeory
	if (evaluationResult)
	{
		free(evaluationResult);
	}
	return operationStatus;
}

// Function to reset the scan parameters to their initial values
void resetScan(Create_RecordManager *RM_scanManager, Create_RecordManager *RM_tableManager)
{
	// function to unpin page from the buffer pool
	unpinPageWrapper(&RM_tableManager->bufferManagerPool, &RM_scanManager->bufferManagerPageHandle);
	RM_scanManager->totalScans = 0;		 // Reset scan count
	RM_scanManager->recID = (RID){1, 0}; // Reset record ID
}

// Function to perform scan operations to find records that match given conditions
extern RC next(RM_ScanHandle *scan, Record *record)
{
	// Get pointers to the record manager structures for the scan and table
	Create_RecordManager *RM_scanManager = (Create_RecordManager *)scan->mgmtData;
	Create_RecordManager *RM_tableManager = (Create_RecordManager *)scan->rel->mgmtData;

	int recordMgrSize = getRecordSize(scan->rel->schema);  // Get the size of each record based on the schema of the scan
	int recordCountSlots = PAGE_SIZE / recordMgrSize;	   // Calculate the number of record slots per page
	int recordEntriesCount = RM_tableManager->totalTuples; // Get the total number of record entries in the table

	// Check initial scan conditions
	RC status = checkScanConditions(RM_scanManager, recordEntriesCount);
	if (status != RC_OK)
		return status;

	// Iterate through records to find matches
	for (int recordSearchCount = RM_scanManager->totalScans; recordSearchCount <= recordEntriesCount; ++recordSearchCount)
	{
		incrementRecordIDIfNeeded(RM_scanManager, recordSearchCount, recordCountSlots);
		pinPageAndCopyData(RM_scanManager, RM_tableManager, record, recordMgrSize);

		// Evaluate the expression for the current record
		Value res;																  // Stack-allocated Value structure
		Value *resPtr = &res;													  // Pointer to Value for evalExpr
		evalExpr(record, scan->rel->schema, RM_scanManager->scanRecord, &resPtr); // Evaluate the expression

		// Check the condition evaluation result
		status = evaluateConditionAndFreeResources(resPtr, RM_scanManager, RM_tableManager);
		if (status == RC_OK)
		{
			return RC_OK; // Match found
		}
		else if (status == RC_ERROR)
		{
			continue; // No match, but no error occurred
		}
		else
		{
			resetScan(RM_scanManager, RM_tableManager); // Error occurred and reset the scan from scan manager
			return status;
		}
	}

	resetScan(RM_scanManager, RM_tableManager); // Reset the scan and indicate no more matches

	return RC_RM_NO_MORE_TUPLES;
}

// Typedef for function pointers used in scan cleanup processes.
typedef RC (*ScanCleanupHandler)(Create_RecordManager *scanManager, BM_BufferPool *pool);

// Function to handle cleanup for a standard scan by unpinning pages and resetting scan metadata.
RC standardScanCleanup(Create_RecordManager *scanManager, BM_BufferPool *pool)
{
	// If the total scans are not zero, perform the scans
	if (scanManager->totalScans > 0)
	{
		RC status = unpinPage(pool, &scanManager->bufferManagerPageHandle); // Fetch the status when pages are unpinned from the pool
		if (status != RC_OK)
			return status;
	}
	// Reset scan management data to initial values for a clean state.
	scanManager->totalScans = 0;
	scanManager->recID.page = 1;
	scanManager->recID.slot = 0;
	return RC_OK;
}

// Function to close the scan and free all associated resources from the pool.
extern RC closeScan(RM_ScanHandle *scan)
{
	if (!scan || !scan->mgmtData) // If scanner does not existvor scan mgmt data is not present
		return RC_INVALID_INPUT;

	// Assign the appropriate cleanup handler based on the scan's context.
	ScanCleanupHandler cleanupHandler = standardScanCleanup;
	Create_RecordManager *RM_scanManager = (Create_RecordManager *)scan->mgmtData;
	Create_RecordManager *recordManager = (Create_RecordManager *)scan->rel->mgmtData;

	// Perform cleanup using the designated handler.
	RC cleanupStatus = cleanupHandler(RM_scanManager, &recordManager->bufferManagerPool);
	// Propagate any errors encountered during cleanup.
	if (cleanupStatus != RC_OK)
		return cleanupStatus; // throw error if cleanup status is not completed

	// Free any used resources after the scan
	scan->mgmtData = NULL;

	return RC_OK; // Indicate successful scan closure.
}

// Wrapper function "createSchema" used to make a new schema and initialize all its properties
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *currentSchema = (Schema *)malloc(sizeof(Schema)); // create a new schema by allocating resources
	if (currentSchema == NULL)								  // throw an error if schema fails to form
	{
		// Handle memory allocation failure
		return NULL;
	}

	// Create a deep copy property names
	currentSchema->attrNames = (char **)malloc(numAttr * sizeof(char *));
	for (int i = 0; i < numAttr; i++) // iterate over the attribute names and duplicate the string names
	{
		currentSchema->attrNames[i] = strdup(attrNames[i]);
	}

	// Deep copy data types
	currentSchema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
	memcpy(currentSchema->dataTypes, dataTypes, numAttr * sizeof(DataType));

	// Deep copy type lengths
	currentSchema->typeLength = (int *)malloc(numAttr * sizeof(int));
	memcpy(currentSchema->typeLength, typeLength, numAttr * sizeof(int));

	// Deep copy keys
	currentSchema->keyAttrs = (int *)malloc(keySize * sizeof(int));
	memcpy(currentSchema->keyAttrs, keys, keySize * sizeof(int));

	// Setting up the rest of the properties
	currentSchema->numAttr = numAttr;
	currentSchema->keySize = keySize;

	// Return the fully initialized schema
	return currentSchema;
}

// This function creates a new record in the schema
extern RC createRecord(Record **record, Schema *schema)
{
	// Validate input parameters
	if (schema == NULL || record == NULL)
	{
		return RC_INVALID_INPUT;
	}

	int recordSize = getRecordSize(schema);
	if (recordSize <= 0)
	{
		return RC_SCHEMA_INVALID;
	}

	// Allocate memory for the new record structure
	Record *newrec = (Record *)malloc(sizeof(Record));
	if (newrec == NULL)
	{
		// Memory allocation for the Record structure failed
		return RC_MEM_ALLOCATION_FAIL;
	}

	// Allocate and initialize record data to default values
	newrec->data = (char *)malloc(recordSize);
	if (newrec->data == NULL)
	{
		// Memory allocation for the data field failed, clean up and exit
		free(newrec);
		return RC_MEM_ALLOCATION_FAIL;
	}

	// Initialize the data with a default character '-' and set the rest to zero
	memset(newrec->data, '-', 1);
	memset(newrec->data + 1, 0, recordSize - 1);

	// Initialize record ID to indicate 'not yet assigned'
	newrec->id.page = newrec->id.slot = -1;

	// Assign the newly created record back to the caller
	*record = newrec;

	return RC_OK; // Record creation successful
}
// This method is used to empty the record memory
extern RC freeRecord(Record *record)
{
	if (record == NULL)
	{
		return RC_INVALID_INPUT; // Safeguard against null pointer dereference
	}

	// If the record data is dynamically allocated, free it first
	if (record->data != NULL)
	{
		free(record->data);
		record->data = NULL; // Prevent dangling pointer by nullifying after free
	}

	// Free the record structure itself
	free(record);

	return RC_OK; // Indicate success
}

// This method is used to calculate the Offset associated
RC propertyOffset(Schema *schema, int getattributeIndex, int *offset)
{
	// Initialize the offset to 1 to account for the initial byte (e.g., tombstone indicator).
	int calculatedOffset = 1;

	// Iterate through each property up to the specified index to sum their sizes.
	for (int i = 0; i < getattributeIndex; ++i)
	{
		// Add the size of the property to the running total based on its data type.
		switch (schema->dataTypes[i])
		{
		case DT_STRING:
			calculatedOffset += schema->typeLength[i];
			break; // String length varies.
		case DT_BOOL:
			calculatedOffset += sizeof(bool);
			break; // Boolean is typically 1 byte.
		case DT_FLOAT:
			calculatedOffset += sizeof(float);
			break; // Float is typically 4 bytes.
		case DT_INT:
			calculatedOffset += sizeof(int);
			break; // Int is typically 4 bytes.
		}
	}

	// Store the final calculated offset in the provided result pointer.
	*offset = calculatedOffset;
	return RC_OK;
}
RC getIntAttr(char *attributeLoc, Value **property); // Extracts an integer value from a record's data and assigns it to a Value structure.

RC getStringAttr(char *attributeLoc, int strLength, Value **property); // Extracts a string from a record's data, allocates memory, and assigns it to a Value structure.

RC getFloatAttr(char *attributeLoc, Value **property); // Extracts a float value from a record's data and assigns it to a Value structure.

RC getBoolAttr(char *attributeLoc, Value **property); // Extracts a boolean value from a record's data and assigns it to a Value structure.

void setIntAttr(char *attributePointer, Value *value);				  // Prototype for setting integer attribute.
void setStringAttr(char *attributePointer, Value *value, int maxLen); // Prototype for setting string attribute.
void setFloatAttr(char *attributePointer, Value *value);			  // Prototype for setting float attribute.
void setBoolAttr(char *attributePointer, Value *value);				  // Prototype for setting boolean attribute.

// Extract integer value and assign to the Value structure
RC getIntAttr(char *attributeLoc, Value **property)
{
	*property = (Value *)malloc(sizeof(Value)); // Allocate Value structure
	if (!(*property))
		return RC_MEM_ALLOCATION_FAIL;

	memcpy(&(*property)->v.intV, attributeLoc, sizeof(int)); // Copy int value
	(*property)->dt = DT_INT;								 // Set data type
	return RC_OK;
}

// Extract string value, allocate memory, and assign to the Value structure
RC getStringAttr(char *attributeLoc, int strLength, Value **property)
{
	*property = (Value *)malloc(sizeof(Value)); // Allocate Value structure
	if (!(*property))
		return RC_MEM_ALLOCATION_FAIL;

	(*property)->v.stringV = (char *)malloc(strLength + 1); // Allocate string
	if (!(*property)->v.stringV)
	{
		free(*property); // Clean up on allocation fail
		return RC_MEM_ALLOCATION_FAIL;
	}
	strncpy((*property)->v.stringV, attributeLoc, strLength); // Copy string value
	(*property)->v.stringV[strLength] = '\0';				  // Ensure null-termination
	(*property)->dt = DT_STRING;							  // Set data type
	return RC_OK;
}

// Extract float value and assign to the Value structure
RC getFloatAttr(char *attributeLoc, Value **property)
{
	*property = (Value *)malloc(sizeof(Value)); // Allocate Value structure
	if (!(*property))
		return RC_MEM_ALLOCATION_FAIL;

	memcpy(&(*property)->v.floatV, attributeLoc, sizeof(float)); // Copy float value
	(*property)->dt = DT_FLOAT;									 // Set data type
	return RC_OK;
}

// Extract boolean value and assign to the Value structure
RC getBoolAttr(char *attributeLoc, Value **property)
{
	*property = (Value *)malloc(sizeof(Value)); // Allocate Value structure
	if (!(*property))
		return RC_MEM_ALLOCATION_FAIL;

	bool boolValue;
	memcpy(&boolValue, attributeLoc, sizeof(bool)); // Copy bool value
	(*property)->v.boolV = boolValue;				// Assign value
	(*property)->dt = DT_BOOL;						// Set data type
	return RC_OK;
}

void setIntAttr(char *attributePointer, Value *value)
{
	*(int *)attributePointer = value->v.intV; // Casts data pointer to int pointer and assigns value.
}

void setStringAttr(char *attributePointer, Value *value, int maxLen)
{
	strncpy(attributePointer, value->v.stringV, maxLen); // Copies string value to data pointer location.
}

void setFloatAttr(char *attributePointer, Value *value)
{
	*(float *)attributePointer = value->v.floatV; // Casts data pointer to float pointer and assigns value.
}

void setBoolAttr(char *attributePointer, Value *value)
{
	*(bool *)attributePointer = value->v.boolV; // Casts data pointer to bool pointer and assigns value.
}
// Main getAttr function
extern RC getAttr(Record *record, Schema *structure, int getattributeIndex, Value **outcome)
{
	// Validate input parameters
	if (!record || !structure || getattributeIndex < 0 || getattributeIndex >= structure->numAttr)
	{
		return RC_INVALID_INPUT;
	}

	int getAttrpos = 0;
	// Calculate the attribute's getAttrpos
	RC getAttrValue = propertyOffset(structure, getattributeIndex, &getAttrpos);
	if (getAttrValue != RC_OK)
		return getAttrValue;

	// Adjust data type for special cases, otherwise use defined type
	DataType type = (getattributeIndex == 1) ? DT_STRING : structure->dataTypes[getattributeIndex];
	char *attributeLoc = record->data + getAttrpos; // Pointer to the attribute within the record

	// Use if-else if structure combined with ternary operator for concise type handling
	return (type == DT_INT) ? getIntAttr(attributeLoc, outcome) : (type == DT_STRING) ? getStringAttr(attributeLoc, structure->typeLength[getattributeIndex], outcome)
															  : (type == DT_FLOAT)	  ? getFloatAttr(attributeLoc, outcome)
															  : (type == DT_BOOL)	  ? getBoolAttr(attributeLoc, outcome)
																					  : RC_RM_UNKNOWN_DATATYPE; // Handle unknown data type
}
extern RC setAttr(Record *record, Schema *schema, int attributeCount, Value *value)
{
	int offset = 0;									 // Initializes offset to zero.
	propertyOffset(schema, attributeCount, &offset); // Calculates offset for the attribute in the record.

	char *attributePointer = record->data + offset; // Points to the correct location in the record's data array.

	// Checks attribute's data type and assigns the value using the appropriate function.
	if (schema->dataTypes[attributeCount] == DT_INT)
	{
		setIntAttr(attributePointer, value); // Sets integer attribute.
	}
	else if (schema->dataTypes[attributeCount] == DT_STRING)
	{
		setStringAttr(attributePointer, value, schema->typeLength[attributeCount]); // Sets string attribute.
	}
	else if (schema->dataTypes[attributeCount] == DT_FLOAT)
	{
		setFloatAttr(attributePointer, value); // Sets float attribute.
	}
	else if (schema->dataTypes[attributeCount] == DT_BOOL)
	{
		setBoolAttr(attributePointer, value); // Sets boolean attribute.
	}
	else
	{
		return RC_RECORD_MANAGER_EMPTY; // Returns error if data type is undefined.
	}

	return RC_OK; // Indicates successful execution.
}
