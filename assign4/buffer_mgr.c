#include "buffer_mgr.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include <math.h>
#include "dberror.h"
#include "buffer_initializer.h"

RC lruk_buffer (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    return RC_OK;
}


BMFrame *checkPinned(BM_BufferPool *const bm, const PageNumber pageNum)
{
    BufferClass *bf = bm->mgmtData;
    BMFrame *pt = bf->head;
 
 
    while (true) {
    if (pt->currpage == pageNum) {
        pt->fixCount++;
        return pt;
    }
    pt = pt->next;
    if (pt == bf->head)
        break;
}


    return NULL;// == return false
}

BufferClass* getBMmgmt(BM_BufferPool *bp){
    return bp->mgmtData;
}

int pinCurrentPage(PageNumber pageNum, BMFrame *pt, BM_BufferPool *const bm )
/*pin page pointed by pt with pageNum-th page. If do not have, create one*/
{
    BufferClass *bf = getBMmgmt(bm);;
    SM_FileHandle fHandle; 
    
    if(openPageFile(bm->pageFile, &fHandle) !=RC_OK) return RC_FILE_OPEN_FAILED;
    
    if(ensureCapacity(pageNum, &fHandle)!=RC_OK) return RC_INVALID_BUFFER_SIZE ;

    
    if (pt->isdirty!=false)
    {
       if (writeBlock(pt->currpage, &fHandle, pt->data) !=RC_OK) {return RC_WRITE_FAILED;}
        
        pt->isdirty = false;
        bf->numWrite++;
    }
    
    if(readBlock(pageNum, &fHandle, pt->data)!=RC_OK) {return RC_FILE_NOT_FOUND;}

    pt->fixCount = pt->fixCount+1;
    bf->numRead = bf->numRead+1;
    pt->currpage = pageNum;
    
    closePageFile(&fHandle);
    
    return 0;
    
    
}

/* Pinning Functions*/

void FIFOSetter(BMFrame *currentFrame ,  BufferClass *bufferManager){
    BMFrame *prev = currentFrame->prev;
    BMFrame *next = currentFrame->next;
    BMFrame *tail = bufferManager->tail;

    prev->next = next;
    next->prev = prev;
    prev = tail;

    tail->next = currentFrame;
    tail = currentFrame;

    next = bufferManager->head;
    bufferManager->head->prev = currentFrame;

}

RC fifo_buffer (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum, bool IsLRU)
{
   
if (!IsLRU && checkPinned(bm, pageNum) ) {
    return RC_OK; // Return success if the page is already pinned
}

// Load the page into memory using the FIFO (First-In-First-Out) replacement policy.
BufferClass *bufferManager = getBMmgmt(bm);;
// Load the current frame in the head
BMFrame *currentFrame = bufferManager->head;

// Find the first available BMFrame in the BufferClass pool.
bool flag = false;
while (currentFrame != bufferManager->head){
 if (currentFrame->fixCount == 0) {
        flag = true;
        break;
    }
    currentFrame = currentFrame->next;
}
if (!flag) return RC_ERROR_PINNING_PAGE;

if(pinCurrentPage(pageNum,currentFrame, bm) !=RC_OK) return RC_ERROR_PINNING_PAGE;

page->data = currentFrame->data;

page->pageNum = pageNum;

// Update the BufferClass management lists to reflect the changes.
if ( bufferManager->head == currentFrame) {
    bufferManager->head = currentFrame->next;
}

FIFOSetter(currentFrame,bufferManager);



return RC_OK; // Return success.

}

void LRUSetter(BufferClass * bf, BMFrame *pt,BM_PageHandle *const page, const PageNumber pageNum){

        BMFrame *prev = pt->prev;
        BMFrame *next = pt->next;
        BMFrame *tail = bf->tail;
        BMFrame *head = bf->head;

    
        prev->next = next;
        next->prev = prev;

        prev = tail;
        tail->next = pt;
        tail = pt;
        
        page->data = pt->data;


        next = head;

        head->prev = pt;
        
        page->pageNum = pageNum;
}

RC lru_buffer (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
/*If not pinned then same as FIFO. If pinned then move to tail.*/
{
    BMFrame *ptr = checkPinned(bm,pageNum);
    BufferClass *bf = getBMmgmt(bm);;
    if (!ptr)
    {
        return fifo_buffer(bm, page, pageNum,true);
    }
    else
    {
        LRUSetter(bf,ptr,page,pageNum);   
    }
    return RC_OK;
}

RC clock_buffer (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BMFrame* isPinned = checkPinned(bm,pageNum);

    if (isPinned!=NULL) return RC_OK;

    BufferClass *bf = getBMmgmt(bm);;
    BMFrame *pt = bf->pointer->next;
    bool notfound = false;
    
    do{
         if (pt->fixCount == 0)
        {
            if (!pt->refbit) //refbit = 0
            {
                notfound = true;
                break;
            }
            pt->refbit = false; //on the way set all bits to 0
        }
        pt = pt->next;
    }
    while (pt!=bf->pointer);
    
    if (!notfound) return RC_IM_NO_MORE_ENTRIES; //no avaliable BMFrame
    
    
    if (pinCurrentPage(pageNum,pt, bm)!=RC_OK) return RC_ERROR;
    
    bf->pointer = pt;
    page->pageNum = pageNum;
    page->data = pt->data;
    
    return RC_OK;
}




void bufferStarter(BufferClass *const bf, const int numPages, void *startData){
    bf->numRead = 0;
    bf->numWrite = 0;
     bf->numFrames = numPages;
    bf->startData = startData;
  
}

RC bufferCreate(BufferClass *const bf , BMFrame *phead,statlist *shead){
    if (phead==NULL || shead ==NULL || bf == NULL) return RC_WRITE_FAILED;
    
    phead->fixCount=0;
    phead->refbit=false;
    phead->isdirty=false;

    memset(phead->data,'\0',PAGE_SIZE);
    shead->fpt = phead;
    bf->head = phead;


    phead->currpage=NO_PAGE;  
    bf->stathead = shead;

    return RC_OK;
    
}

RC initBufferPool(BM_BufferPool *const bm, const char *const fileName, const int numPages, ReplacementStrategy strat,  void *startData)
//initialization: create page frames using circular list; init bm;
{
    BufferClass *bf = calloc(bm->numPages ,sizeof(BufferClass));

    //error check
    if (numPages<=0)   return RC_WRITE_FAILED;
    //init bf:bookkeeping data
    
    if (bf==NULL) return RC_BUFFER_NOT_INIT;
   
    bufferStarter(bf,numPages,startData);
    //create list
    int k=0;
    statlist *shead = malloc( sizeof(statlist));
    BMFrame *phead = malloc(sizeof(BMFrame));
    if (phead==NULL) return RC_WRITE_FAILED;

   bufferCreate(bf, phead,shead);

    while (++k<numPages) { 
        BMFrame *newFrame = malloc(sizeof(BMFrame));
        statlist *newStatList = malloc(sizeof(statlist));

        if (newFrame!=NULL) {
        
        
        newFrame->currpage=NO_PAGE;     
        newFrame->isdirty=false;   
        newFrame->refbit=false;  
        newFrame->fixCount=0;
        
        newStatList->fpt = newFrame;

        shead->next = newStatList;
        shead = newStatList;        
        phead->next=newFrame;


        newFrame->prev=phead;
        phead=newFrame;

        memset(newFrame->data,'\0',4096);
        }

        else
        return RC_WRITE_FAILED;

    }
    bm->mgmtData = bf;
    bm->pageFile = (char *)fileName;

    shead->next = NULL;
    bf->tail = phead;
    bf->pointer = bf->head;

    BufferClass *aux = bf;
    BMFrame *tail = aux->tail;
    BMFrame *head = aux->head;
    
    tail->next = aux->head;
    head->prev = tail;
    
    //init bm
    bm->strategy = strat;
   
    bm->numPages = numPages;
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    BufferClass *bf = getBMmgmt(bm);;
    BMFrame *pt = bf->head;
    RC flushValue = forceFlushPool(bm);

    if (flushValue!=RC_OK) {
        return flushValue;
    }
   
    
    do{
    pt = pt->next;
        free(bf->head);
        bf->head = pt;
    }
    while (pt!=bf->tail);

    free(bf->tail);
    free(bf);


    bm->pageFile = NULL;
    bm->mgmtData = NULL;    
    bm->numPages = 0;

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    SM_FileHandle fHandle;

    if (openPageFile(bm->pageFile, &fHandle)!=RC_OK) {
        return RC_ERROR;
    }
    
    
    BufferClass *bf = getBMmgmt(bm);;
    BMFrame *pt = bf->head;
    while (pt!=bf->head)
    {
        if (pt->isdirty==true)
        {
            RC writeValue = writeBlock(pt->currpage, &fHandle, pt->data);

            if (writeValue!=RC_OK){
                return writeValue;
            }
            bf->numWrite++;
            pt->isdirty = false;
        }
        pt = pt->next;
    };
    
    closePageFile(&fHandle);

    return RC_OK;
}

// Buffer  Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    BufferClass *bf = getBMmgmt(bm);;
    BMFrame *pt = bf->head;
    
    do{
         pt=pt->next;
        if (pt==bf->head)
            return RC_READ_NON_EXISTING_PAGE;
    }while (pt->currpage!=page->pageNum);
    
    pt->isdirty = true;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)

{
    BufferClass *bf = getBMmgmt(bm);;
    BMFrame *pt = bf->head;
    
    do{
        pt=pt->next;
        if (pt==bf->head)
            return RC_READ_NON_EXISTING_PAGE;
    }
    while (pt->currpage!=page->pageNum);
    
    if (pt->fixCount == 0)
        pt->refbit = false;

    if (pt->fixCount > 0)
    {
        pt->fixCount--;        
    }
    else
        return RC_READ_NON_EXISTING_PAGE;

    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //current frame2file
    BufferClass *bf = getBMmgmt(bm);;
    SM_FileHandle fHandle;
    if(openPageFile(bm->pageFile, &fHandle) !=RC_OK) return RC_FILE_NOT_FOUND ;

    
    if(writeBlock(page->pageNum, &fHandle, page->data) !=RC_OK)
    {
        closePageFile(&fHandle);
        return RC_FILE_NOT_FOUND;
    }
    
    bf->numWrite = bf->numWrite + 1;
    closePageFile(&fHandle);
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,  const PageNumber pageNum)
{
    if (pageNum>=0){

     if(bm->strategy == RS_CLOCK){
        return clock_buffer(bm,page,pageNum);
     }
     else if(bm->strategy == RS_LRU_K){
        return lruk_buffer(bm,page,pageNum);
     }
     else if(bm->strategy == RS_LRU){
        return lru_buffer(bm,page,pageNum);
     }  
    else if(bm->strategy == RS_FIFO){
        return fifo_buffer(bm,page,pageNum,false);
     }      

     return RC_IM_KEY_NOT_FOUND;
                     
    }

    return RC_IM_KEY_NOT_FOUND;
}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageNumber *arr = malloc(sizeof(int));
    BufferClass *bf = getBMmgmt(bm);
    statlist *sptr = bf->stathead;
    int count = 0 ;
    
    while (count<bm->numPages)
    {

        int curr = sptr->fpt->currpage;
        arr[count++]=curr;
        sptr=sptr->next;
    }

    return arr;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *flag = malloc(sizeof(bool)); 
    BufferClass *bf = getBMmgmt(bm);
    statlist *pntr = bf->stathead;
    int count=0;
    while (pntr!=NULL && count<bm->numPages)
    {
        bool searchdirty = pntr->fpt->isdirty;
        if (searchdirty)
        { 
            flag[count++]=1;
        }

        pntr=pntr->next;
    }

    return flag;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    BufferClass *bf = getBMmgmt(bm);
    statlist *spt = bf->stathead;
    int count = 0;
    PageNumber *pg = malloc(sizeof(int)); 
    while(count++<bm->numPages){
    pg[count]=spt->fpt->fixCount;
        spt=spt->next;
    }
    return pg;
}

