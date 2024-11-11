#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"
#include "stdlib.h"
/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_RECORD_MANAGER_EMPTY -1      // Added a new definition for ERROR
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_ERROR 400                     // Added a new definition for ERROR
#define RC_PINNED_PAGES_IN_BUFFER 500    // Added a new definition for Buffer Manager
#define RC_INVALID_NUM_PAGES 501         // Added a new definition for Buffer Manager
#define RC_EVEN_NUM_PAGES 502            // Added a new definition for Buffer Manager
#define RC_LARGE_BUFFER_SIZE 503         // Added a new definition for Buffer Manager
#define RC_TOO_MANY_WRITE_OPERATIONS 504 // Added a new definition for Buffer Manager
#define RC_TOO_FEW_WRITE_OPERATIONS 505  // Added a new definition for Buffer Manager
#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKNOWN_DATATYPE 205        // Corrected the typo in the error code name
#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TOO_LARGE 302              // Corrected the typo in the error code name
#define RC_IM_NO_MORE_ENTRIES 303
#define RC_INVALID_ARGUMENT 506            // Added a new definition for Invalid Argument
#define RC_INVALID_INPUT 305               // Added a new definition for Invalid Argument
#define RC_BUFFER_NOT_INIT 507             // Added a new definition for Buffer Not Initialized
#define RC_PAGE_NOT_FOUND 508              // Added a new definition for Page Not Found
#define RC_BUFFER_POOL_NOT_INITIALIZED 509 // Added a new definition for Buffer Pool Not Initialized
#define RC_INVALID_BUFFER_SIZE 510         // Added a new definition for Invalid Buffer Size
#define RC_TOO_LARGE_BUFFER_SIZE 511       // Added a new definition for Too Large Buffer Size
#define RC_NEGATIVE_FIX_COUNT 512          // Added a new definition for Negative Fix Count
#define RC_BUFFER_NOT_INITIALIZED 513      // Added a new definition for Buffer Not Initialized
#define RC_FILE_OPEN_FAILED 514            // Added a new definition for File Open Failed
#define ERROR_INVALID_POOL 1000            // Added a new definition for Invalid Pool
#define ERROR_MEMORY_ALLOCATION 1001       // Added a new definition for Memory Allocation
#define RC_BM_NOT_EXIST 999                // Added a new definition for Buffer Pool
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID  604
#define RC_SCAN_CONDITION_NOT_FOUND 605
#define RC_MEMORY_ALLOCATION_FAILURE 606 // Added a new definition for memory allocation failure for record manager
#define RC_ERROR_PINNING_PAGE 607 // Added a new definition for pinng page failure for record manager
#define RC_SCHEMA_INVALID 701     // Added a new definition for schema invalid error
#define RC_MEM_ALLOCATION_FAIL 702  // Added a new definition for memory allocation failure
#define RC_INVALID_RECORD_SIZE 703 // Added a new definition for invalid record size
#define RC_CONTINUE 704 // Added a new definition for continuing parsing records
#define RC_FILE_DELETION_FAILED 801

// Added new definition for B-Tree
#define RC_ORDER_TOO_HIGH_FOR_PAGE 7001
#define RC_INSERT_ERROR 7002
#define RC_NO_RECORDS_TO_SCAN 7003
#define RC_MEMORY_ALLOCATION_ERROR 5001
#define RC_IM_MEMORY_ERROR 6001
#define RC_MALLOC_FAILED 70001
#define RC_MEMORY_ALLOCATION_FAILED 705

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
