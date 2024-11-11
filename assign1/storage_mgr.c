#include "storage_mgr.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"



void initStorageManager (void)
{

}
//Function : Create Page File
// Description : This function creates a page file
/* This code here checks if the file name created is valid or else it returns file is not found
Then it checks if the files can be read and written and adds headers to the file
Then it frees up allocated memory from the headers and closes the files and returns the result*/

RC createPageFile(char *fileName)
{
    if (fileName == NULL) 
    {
        return RC_FILE_NOT_FOUND; // Returns file is not found if the fileName is Null
    }

    FILE *files = fopen(fileName, "w+"); // Opens the file in read and write mode
    if (files == NULL)
    {
        return RC_FILE_NOT_FOUND; // Returns file is not found if the file cannot be opened in read and write mode
    }
    RC result = RC_FILE_NOT_FOUND; // Initialising file not found to the result
    char *Header = NULL;

    do {
        if (fprintf(files, "1") < 0) // Using do while and then writing 1 at the beginning of the file to make it non empty
        {
            break; // If the writing fails then exit
        }
        Header = (char*)calloc(2 * PAGE_SIZE, sizeof(char)); // Allocating memory for the Header
        if (Header == NULL)
        {
            break; // If the memory allocation fails then exit
        }
        if (fwrite(Header, PAGE_SIZE, 2, files) != 2) // Write the header to the file
        {
            break; // If the writing fails then exit
        }

        result = RC_OK; // If all the operations are successfull return OK
    } while (0);

    if (Header != NULL) // If allocated memory exists it cleans it up
    {
        free(Header);
    }
    fclose(files); // CLose all the opened files
    return result; // Return the result
}

// Function : Open Page File
// Description : This function opens a page file
/*This function opens a file and then read the first page of the file*/
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    if (fileName == NULL || fHandle == NULL)
    {
        return RC_FILE_NOT_FOUND; // Checks if the filaname and fhandle is valid or not and if its NULL returns file not found
    }   
    FILE *files = fopen(fileName, "r+");
    if (files == NULL)
    {
        return RC_FILE_NOT_FOUND; //Returns file is not found if the file cannot be opened in read and write mode
    }
    fHandle->fileName = fileName; // Initialising file handle properties
    fHandle->mgmtInfo = files;
    fHandle->curPagePos = 0;

    char headerBuffer[PAGE_SIZE] = {0}; // 
    if (fgets(headerBuffer, PAGE_SIZE, files) == NULL) // Read the first line of the file and gets the total page size
    {
        fclose(files);
        return RC_FILE_NOT_FOUND; //Returns file is not found if the reading fails
    }
    fHandle->totalNumPages = atoi(headerBuffer); // Read string is converted to an integer
    if (fHandle->totalNumPages <= 0)
    {
        fclose(files);
        return RC_FILE_NOT_FOUND; //Returns file is not found if the total number of pages is 0
    }
    return RC_OK; // Returns OK if the operations are successfull
}

// Function : ClosePageFile
// Description: It closes the page file
/*This function closes a page in the file after checking if the file handle is valid or not*/
RC closePageFile(SM_FileHandle *fHandle)
{
    RC result = RC_OK;
    if (fHandle == NULL)
    {
        return RC_FILE_NOT_FOUND; //Checks if the File Handle is valid and if the File Handle is NULL returns file not found
    }
    if (fHandle->mgmtInfo != NULL) //Checks if the management info exists
    {
        if (fclose(fHandle->mgmtInfo) == 0) //Attempts to close the file
        {
            fHandle->mgmtInfo = NULL; // Sets the management info to NULL if its successfull in closing the file
        }
        else
        {
            result = RC_FILE_NOT_FOUND; // Returns file not found if its unsuccessful in closing the file
        }
    }

    return result; // Returns the result if the operations are successful
}

// Function : DestroyPageFile
// Description: It destroys the page file
/* This function attempts to remove the file
IF its succesfull in removing the file it returns OK
IF its unsucessfull in removing the file it returns File Not Found
If the filename is NULL then it also returns File Not Found*/
RC destroyPageFile(char *fileName)
{
    RC result = RC_FILE_NOT_FOUND;
    if (fileName != NULL) // Checks if the file name is not NULL 
    {
        if (remove(fileName) == 0) // Attempts to the remove the file
        {
            result = RC_OK; // If its successfull in removing the file it returns OK
        }
    }

    return result; // Returns the result if the operations are successful
}

// Function : Read Block
// Description: It reads a block
/* This function reads the page if its within the total number of pages
and then reads the size of the pages*/
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) // Set some input parameters
{
    if (fHandle == NULL || memPage == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_READ_NON_EXISTING_PAGE; // Returns Read non existing page if the input parameters are NULL
    }
    if (pageNum < 0 || pageNum > fHandle->totalNumPages - 1) // Checks if the page to be read is within the bounds of the total  number of pages
    {
        return RC_READ_NON_EXISTING_PAGE; // Returns Read non existing page if its out of bounds
    }
    long offset = (pageNum + 1) * PAGE_SIZE;
    FILE *files = fHandle->mgmtInfo; // 1 is added to the pageNum to account for the Header File

    if (fseek(files, offset, SEEK_SET) == 0)
    {
        size_t bytesRead = fread(memPage, 1, PAGE_SIZE, files); // Page_Size bytes is read from the files to memPage
        if (bytesRead == PAGE_SIZE)
        {
            fHandle->curPagePos = pageNum; 
            return RC_OK; // Returns Ok if the read is successfull
        }
    }
    
    return RC_READ_NON_EXISTING_PAGE;// Returns Read non existing page if the read is unsuccessful or page is not found
}

/* * Returns the file's current page position. 
* * This function certifies the validity of the file handle. If it is genuine, it returns the current page position (curPagePos) that is kept in the file handle.
 * * It gives an error code indicating the page does not exist if the file handle is NULL.  * */

int getBlockPos(SM_FileHandle *fHandle) {
    if (fHandle == NULL) {
        return RC_READ_NON_EXISTING_PAGE;  // Return an invalid position if the file handle is NULL
    }
    else {
        return fHandle->curPagePos;  // Return the current page position from the file handle
    }
}
/* * Read the file's the beginning block, or page 0, into memory. The block at position 0 is read by calling the readBlock() method.
 * */

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

/* * Reads the file's end block into memory. It first tests that the file handle is authentic. If it is genuine, it reads the last block and determines its position (totalNumPages - 1).
 * */

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_READ_NON_EXISTING_PAGE;  // Return error if file handle is NULL
    }
    else {
        int Last_Block_POS = fHandle->totalNumPages - 1;  // Calculate last block position
        return readBlock(Last_Block_POS, fHandle, memPage);  // Read last block
    }
}
/* * Read the previous block relative to the current page position in the file. 
 * It first validates that the file handle is valid. If it is authentic, it reads the previous block and determines its location (current position - 1).
 * */

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_READ_NON_EXISTING_PAGE;  // Return error if file handle is NULL
    }
    else {
        int Previous_Block_POS = getBlockPos(fHandle) - 1;  // Calculate previous block position
        return readBlock(Previous_Block_POS, fHandle, memPage);  // Read previous block
    }
}
/* * Read the current block from the file at the current page position.
 * It first verifies that the file handle is valid. If it is correct, it reads the block and gets the current page position.
 * */

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_READ_NON_EXISTING_PAGE;  // Return error if file handle is NULL
    }
    else {
        int Current_Block_POS = getBlockPos(fHandle);  // Get current block position
        return readBlock(Current_Block_POS, fHandle, memPage);  // Read current block
    }
}

// Author : Sanketkumar Patel | A20523237
// Function : readNextBlock
// Description : This function reads next block.
// Inputs : SM_FileHandle *fHandle, SM_PageHandle memPage
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //Checking if file handle is valid or not
		if (fHandle == NULL) {
            //returning an error code if the file handle is not initialized.
            return RC_FILE_HANDLE_NOT_INIT; 
	}

    //after checking valid file handle is valid, reading desired Block
	else {
        //Calculating position of next block using already implemented getBlockPos function.
		int Next_Block_POS= getBlockPos(fHandle)+1; 

        //Reading block using already implemented readBlock function
		return readBlock(Next_Block_POS,fHandle,memPage); 

        //readBlock function will take care about the following
        //If the user tries to read a block before the first page or after the last page of the file, it will return RC READ_NON_EXISTING_PAGE
        //The curPagePos will be moved to the page that was read.
    }
}

// Author : Sanketkumar Patel | A20523237
// Function : writeBlock
// Description : This function writes a page to disk using an absolute position.
// Inputs : int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Checking if the file handle, memory page, or management information is valid or not
	    if (fHandle == NULL || memPage == NULL || fHandle->mgmtInfo == NULL)
    {
        //returning write failed error code if the file handle is not initialized.
        return RC_WRITE_FAILED; 
    }

    // Checking if the input page number is within range or not
    if (pageNum < 0 || pageNum > fHandle->totalNumPages - 1) 
    {
        //returning write failed error if the input page in not within range
        return RC_WRITE_FAILED; 
    }

	
        // Seek to the specified page
		fseek(fHandle->mgmtInfo,(pageNum+1)*PAGE_SIZE,SEEK_SET);

        // Write page to the file
		fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);

        //updating curPagePos
		fHandle->curPagePos = pageNum;

        //Returning Success
		return RC_OK;
}

// Author : Sanketkumar Patel | A20523237
// Function : writeCurrentBlock
// Description : This function writes a page to disk using a current position.
// Inputs : SM_FileHandle *fHandle, SM_PageHandle memPage
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //Checking if file handle is valid or not
	if (fHandle == NULL) {
        //returning an error code if the file handle is not initialized.
		return RC_FILE_HANDLE_NOT_INIT;
	}
    //after checking valid file handle is valid, writing desired Block. 
	else {
        //Calculating position of current block using already implemented getBlockPos function.
		int Current_Block_POS= getBlockPos(fHandle);

        //Writing block using already implemented writeBlock function
		return writeBlock(Current_Block_POS,fHandle,memPage);

        //writeBlock function will take care about the following
        //Null check of file handle, memory page, or management information
        //Write block
        //updating curPagePos
	}
}

// Author : Sanketkumar Patel | A20523237
// Function : appendEmptyBlock
// Description : This function increase the number of pages in the file by one. The new last page will be filled with zero bytes.
// Inputs : SM_FileHandle *fHandle
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	// Ensuring file handle is valid or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Allocating memory for new empty block
    SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    if (emptyBlock == NULL) {
        return RC_WRITE_FAILED;
    }

    // Moving file pointer to end of file
    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, 0, SEEK_END) != 0) {
        free(emptyBlock);
        return RC_WRITE_FAILED;
    }

    // Writing empty block to file
    size_t written = fwrite(emptyBlock, sizeof(char), PAGE_SIZE, file);
    if (written != PAGE_SIZE) {
        free(emptyBlock);
        return RC_WRITE_FAILED;
    }

   
    free(emptyBlock);  // Free allocated memory for empty block

    // Updating file handle to reflect the newly added page anf to move curPagePos to new page
    fHandle->totalNumPages++;
    fHandle->curPagePos = fHandle->totalNumPages - 1;

    // Update the header of the file (first page) to store the new total number of pages
    rewind(file);
    fprintf(file, "%d", fHandle->totalNumPages);

    //returning Success
    return RC_OK;
}

// Author : Sanketkumar Patel | A20523237
// Function : ensureCapacity
// Description : This function ensures if the file has less than numberOfPages pages then increase the size to numberOfPages.
// Inputs : int numberOfPages, SM_FileHandle *fHandle
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    // Validating existance of file handle & management information
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        //returning an error code if the file handle is not initialized.
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Checking if current number of pages is already sufficient or not
    if (fHandle->totalNumPages >= numberOfPages) {
        //returning success if the file already has sufficient number of pages
        return RC_OK;
    }

    // if no success in above check then Adding empty blocks until the number of pages matches numberOfPages
    while (fHandle->totalNumPages < numberOfPages) {
        // Adding empty block to the file using already implemented appendEmptyBlock
        RC status_to_append = appendEmptyBlock(fHandle);
        if (status_to_append != RC_OK) {
            return status_to_append;
        }
    }

    //returning Success
    return RC_OK;
}