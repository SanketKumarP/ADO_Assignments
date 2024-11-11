#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"


typedef struct Tree_Node
{
    void **totalPointers;
    int node_countKeys;
    int currNodeposn;
    Value *keys;
    bool Node_Leaf;
    struct NodeTree *nodePointer;
    
    union
    {
        struct
        {
            FILE *file;
            int *offsets;
        } leaf;

        struct
        {
            int max_children;
        } non_leaf;
    } node_data;
} Tree_Node;

typedef struct Tree_Scan
{
    Tree_Node *present_Node;
    int scanResult;
    int scanPosition;

    union
    {
        struct
        {
            Tree_Node *present_Node;
            int scanResult;
            int scanPosition;
        } current;

        struct
        {
            Tree_Node *currentNode;
            int scanResults;
            int currentscanPosition;
        } legacy;
    };
} Tree_Scan;
typedef struct Tree_Data
{
    BM_BufferPool *bufferPool;
    int bufferEntry;
    int largestKey;

    union
    {
        struct
        {
            BM_BufferPool *bufferPool;
            int entries;
            int maxKey;
        } current;

        struct
        {
            BM_BufferPool *pool;
            int total;
            int maxKeyVal;
        } legacy;
    };
} Tree_Data;

int node_index = 0;
int totalList = 0;
Tree_Node *treeRoot = NULL;
char *closeBTree1 = NULL;
Value emptyList;
int reclist_size = 0;
char *closeBTree2 = NULL;
void copyNodes(Tree_Node **newNodePointers, Tree_Node **existingPointers, int maxSize, int insertPosition, Tree_Node *newRightNode);
RC insertIntoNode(Tree_Node *node, int position, Value newKey, Tree_Node *rightChild);
RC insertParent(Tree_Node *leftChild, Tree_Node *rightChild, Value newKey);
void copyKeys(Value *newKeyArray, Value *existingKeys, int maxSize, int insertPosition, Value newKey);
void splitNodeIntoArrays(Tree_Node *currentNode, Tree_Node *newRightNode, int insertIndex, Value newKey, int maxArraySize, Tree_Node **tempPointerArray, Value *tempKeyArray);
void copyNodePointers(Tree_Node *targetNode, Tree_Node **tempPointerArray, int midpoint);
void copyNodeKeys(Tree_Node *targetNode, Value *tempKeyArray, int midpoint);
RC splitNode(Tree_Node *nodePointer, int scanPosition, Value key, Tree_Node *right, int reclist_size);
void removeKeyAndPointer(Tree_Node *bNodeTree, int scanPosition, int NumKeys, bool isLeaf);
RC handleRootUnderflow(Tree_Node **treeRootPtr, int totalList);
RC deleteNode(Tree_Node *parentNode, int nodePosition);
RC mergeNodes(Tree_Node *parentNode, Tree_Node *bNodeTree, Tree_Node *brother, 
              int nodePosition, int NumKeys, int reclist_size, int totalList);
void shiftNodeContents(Tree_Node *bNodeTree, Tree_Node *brother, 
                      Tree_Node *parentNode, int nodePosition, int NumKeys);
RC adjustNodesAfterDeletion(Tree_Node *parentNode, Tree_Node *bNodeTree, Tree_Node *brother, int nodePosition, int NumKeys);
RC deleteNode(Tree_Node *bNodeTree, int scanPosition);

// In btree_helper.h or btree_mgr.h
extern RC createParent(Tree_Node *leftChild, Tree_Node *rightChild, Value newKey);
extern RC splitNode(Tree_Node *parentNode, int positionIndex, Value newKey, Tree_Node *rightChild, int reclist_size);
extern Tree_Node *createNode();

