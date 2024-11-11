#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <stdlib.h>
typedef struct BMFrame{
    int currpage; //the corresponding page in the file
    struct BMFrame *next;
    char data[PAGE_SIZE];
    struct BMFrame *prev;
    bool isdirty;
    bool refbit; //true=1 false=0 for clock
    int fixCount;
    
} BMFrame;
typedef struct statlist{
    BMFrame *fpt; //BMFrame pt
    struct statlist *next;
}statlist;
typedef struct BufferClass{ //use as a class
    BMFrame *head;
    BMFrame *tail;
    int numWrite; //for writeIO
    void *startData;
    int numFrames; // number of frames in the BMFrame list
    int numRead; //for readIO  
    
    BMFrame *pointer; //special purposes;init as bfhead;clock used
    statlist *stathead; //statistics functions have to follow true sequence -.-|
}BufferClass;



