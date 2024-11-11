#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "storage_mgr.h"

FILE *pageFile;

extern void initStorageManager(void)
{
}

RC createPageFile(char *fileName)
{
    FILE *file = fopen(fileName, "w");

    if (!file)
    {
        return RC_WRITE_FAILED;
    }

    if (fwrite("\0", sizeof(char), PAGE_SIZE, file) < PAGE_SIZE)
    {
        fclose(file);
        return RC_WRITE_FAILED;
    }

    fclose(file);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *file = fopen(fileName, "r+");

    if (!file)
    {
        return RC_FILE_NOT_FOUND;
    }

    fseek(file, 0, SEEK_END);
    fHandle->totalNumPages = ftell(file) / PAGE_SIZE;

    fseek(file, 0, SEEK_SET);
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;

    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle)
{
    RC result = (fHandle == NULL || fHandle->mgmtInfo == NULL) ? RC_FILE_HANDLE_NOT_INIT : RC_OK;

    FILE *file = (FILE *)fHandle->mgmtInfo;

    fHandle->fileName = result == RC_OK ? NULL : fHandle->fileName;
    fHandle->curPagePos = result == RC_OK ? 0 : fHandle->curPagePos;
    fHandle->totalNumPages = result == RC_OK ? 0 : fHandle->totalNumPages;
    fHandle->mgmtInfo = result == RC_OK ? NULL : fHandle->mgmtInfo;

    fclose(file);

    return result;
}

RC destroyPageFile(char *fileName)
{
    FILE *file = fopen(fileName, "r");

    if (!file)
    {
        fclose(file);
        return RC_FILE_NOT_FOUND;
    }

    fclose(file);

    return (remove(fileName) == 0) ? RC_OK : RC_FILE_NOT_FOUND;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;

    FILE *getFile = fopen(fHandle->fileName, "r");

    long position = pageNum * PAGE_SIZE;
    int seekPosition = fseek(getFile, position, SEEK_SET);

    if (seekPosition == 0)
    {
        if (fread(memPage, sizeof(char), PAGE_SIZE, getFile) < PAGE_SIZE)
            return RC_FILE_NOT_FOUND;
    }
    else
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    fHandle->curPagePos = ftell(getFile);

    fclose(getFile);

    return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(0, fHandle, memPage) == RC_OK)
        return RC_OK;

    return RC_READ_NON_EXISTING_PAGE;
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(fHandle->curPagePos - 1, fHandle, memPage) == RC_OK)
        return RC_OK;

    return RC_READ_NON_EXISTING_PAGE;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
    {
        return RC_OK;
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(fHandle->curPagePos + 1, fHandle, memPage) == RC_OK)
    {
        return RC_OK;
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (readBlock(fHandle->totalNumPages - 1, fHandle, memPage) == RC_OK)
    {
        return RC_OK;
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (pageNum < 0 || pageNum > fHandle->totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *getFilePage = fopen(fHandle->fileName, "r+");

    long pageoffset = (pageNum)*PAGE_SIZE;

    if (pageNum == 0)
    {
        fseek(getFilePage, pageoffset, SEEK_SET);
        int pg = 0;
        while (pg < PAGE_SIZE)
        {
            if (feof(getFilePage))
                appendEmptyBlock(fHandle);

            fputc(memPage[pg], getFilePage);
            pg++;
        }

        fHandle->curPagePos = ftell(getFilePage);

        fclose(getFilePage);
    }
    else
    {
        fHandle->curPagePos = pageoffset;
        fclose(getFilePage);
        writeCurrentBlock(fHandle, memPage);
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int currentBlockPos = getBlockPos(fHandle);

    if (writeBlock(currentBlockPos, fHandle, memPage) == RC_WRITE_FAILED)
    {
        return RC_WRITE_FAILED;
    }

    fHandle->curPagePos++;
    return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    SM_PageHandle empty_page = (char *)calloc(PAGE_SIZE, sizeof(char));

    if (empty_page == NULL)
    {
        return RC_WRITE_FAILED;
    }

    if (fwrite(empty_page, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE)
    {
        free(empty_page);
        return RC_WRITE_FAILED;
    }

    fHandle->totalNumPages = fHandle->totalNumPages + 1;
    fHandle->curPagePos = fHandle->totalNumPages - 1;

    free(empty_page);
    empty_page = NULL;
    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int currentPages = fHandle->totalNumPages;

    if (numberOfPages <= currentPages)
    {
        return RC_OK;
    }

    if (fHandle->mgmtInfo != NULL)
    {
        int iterations = (fHandle->totalNumPages < numberOfPages) ? (numberOfPages - fHandle->totalNumPages) : 0;

        if (fHandle->totalNumPages < numberOfPages)
        {
            for (int i = 0; i < iterations && appendEmptyBlock(fHandle) == RC_OK; ++i)
                ;
        }

        return RC_OK;
    }

    return RC_FILE_NOT_FOUND;
}
