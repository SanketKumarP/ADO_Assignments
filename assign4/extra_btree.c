#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "btree_mgr.h"
#include "dberror.h"
#include "extra_btree.h"
#define LINESIZE 100
#define MAX_KEYS 100

// Function to retrieve the Record Identifier (RID) pointer from the node's `totalPointers` array at index `i`.
RID *accessPointer(Tree_Node *leaf, int i)
{
    // Access the i-th pointer in the `totalPointers` array of the given node (`leaf`).
    // This array holds pointers to child nodes or RID structures, depending on the node type.
    return (RID *)leaf->totalPointers[i]; // Cast and return the pointer as an RID type
}


// Function to serialize a key within a node and store the result in global variables based on node type.
void serialize(Tree_Node *leaf, char nodeType, int idx, Value *key)
{
    char *serializedStr; // Pointer to hold the serialized string

    // Check if the node type is a leaf ('l') or an internal node
    if (nodeType == 'l')
    {
        // Serialize the key at the specified index in the leaf node
        serializedStr = serializeValue(&leaf->keys[idx]);
        
        // Store the serialized result in global variable `closeBTree1` for leaf nodes
        closeBTree1 = serializedStr;
    }
    else
    {
        // Serialize the provided key for non-leaf nodes
        serializedStr = serializeValue(key);

        // Store the serialized result in global variable `closeBTree2` for non-leaf nodes
        closeBTree2 = serializedStr;
    }
}


// Retrieve a pointer to a node from the `totalPointers` array at a specified index `i` within the given node.
// This can point to either a leaf or internal child node.
Tree_Node *getTNLeafPointers(Tree_Node *node, int i)
{
    // Access the `i`th pointer from the `totalPointers` array within the `node`.
    return node->totalPointers[i];
}

// Recursive function to perform depth-first traversal and assign unique positions to each node in the tree,
// starting from the `root` node.
int DFS(Tree_Node *root)
{
    // Base case: stop recursion if the node is NULL or if it's a leaf node.
    if (root == NULL || root->Node_Leaf)
    {
        return 0; // Return 0 as the end of recursion for NULL or leaf nodes
    }

    // Assign a unique position to the current node and increment the global `node_index` counter.
    root->currNodeposn = node_index++;

    // Loop through all keys in the current node to recursively traverse its child nodes.
    for (int i = 0; i < root->node_countKeys; i++)
    {
        DFS(root->totalPointers[i]); // Recursive call for each child node
    }
    
    return 0; // Return 0 to indicate successful completion of traversal
}



// Concatenates the `src` string to the end of the `dest` string.
// Note: Ensure `dest` has enough space to hold the result.
void concatstr(char *dest, char *src)
{
    strcat(dest, src); // Append `src` to `dest`
}


// Frees memory for the global serialized strings `closeBTree1` or `closeBTree2` based on the `option` parameter.
// - option 1: Free `closeBTree1`
// - option 2: Free `closeBTree2`
// - any other value: Free both `closeBTree1` and `closeBTree2`
void freeTree(int option)
{
    switch (option)
    {
        case 1:
            free(closeBTree1);       // Free `closeBTree1`
            closeBTree1 = NULL;      // Reset pointer to NULL
            break;
        case 2:
            free(closeBTree2);       // Free `closeBTree2`
            closeBTree2 = NULL;      // Reset pointer to NULL
            break;
        default:
            free(closeBTree1);       // Free both `closeBTree1` and `closeBTree2`
            free(closeBTree2);
            closeBTree1 = NULL;      // Reset both pointers to NULL
            closeBTree2 = NULL;
            break;
    }
}



// Function to serialize and scan a B-tree starting from `root`, appending the result to `output`.
int scanTree(Tree_Node *root, char *output)
{
    char lineBuffer[LINESIZE];                       
    sprintf(lineBuffer, "(%d)[", root->currNodeposn); // Start with node's position

    if (root->Node_Leaf) // Leaf node serialization
    {
        int k = 0;
        while (k++ < root->node_countKeys) 
        {
            // Append RID (Record Identifier) to `lineBuffer`
            sprintf(lineBuffer + strlen(lineBuffer), "%d.%d,",
                    accessPointer(root, k)->page,
                    accessPointer(root, k)->slot);

            // Serialize the key at index `k` and append it to `lineBuffer`
            serialize(root, 'l', k, &root->keys[k]);
            concatstr(lineBuffer, closeBTree1);
            concatstr(lineBuffer, ",");
            freeTree(1); // Free memory used by serialized key
        }

        // Get next leaf node, if any, and append its position; remove trailing comma if none
        Tree_Node *nextLeaf = getTNLeafPointers(root, reclist_size - 1);
        if (nextLeaf == NULL)
        {
            lineBuffer[strlen(lineBuffer) - 1] = 0;
        }
        else
        {
            sprintf(lineBuffer + strlen(lineBuffer), "%d", nextLeaf->currNodeposn);
        }
    }
    else // Non-leaf node serialization
    {
        int i = 0;
        while (i++ <= root->node_countKeys) 
        {
            // Append child node position
            Tree_Node *childNode = getTNLeafPointers(root, i);
            sprintf(lineBuffer + strlen(lineBuffer), "%d,", childNode->currNodeposn);

            // Serialize the current key and append to `lineBuffer`
            serialize(root, 'l', i, NULL);
            strcat(lineBuffer, closeBTree1);
            freeTree(1);
            concatstr(lineBuffer, ",");
        }

        // Handle the last child to avoid trailing comma
        Tree_Node *lastChild = getTNLeafPointers(root, root->node_countKeys);
        if (lastChild != NULL)
        {
            sprintf(lineBuffer + strlen(lineBuffer), "%d", lastChild->currNodeposn);
        }
        else
        {
            lineBuffer[strlen(lineBuffer) - 1] = 0;
        }
    }

    // Close the node entry and append it to the final output
    strcat(lineBuffer, "]\n");
    strcat(output, lineBuffer);

    // Recursively serialize child nodes if not a leaf
    if (!root->Node_Leaf) 
    {
        int i = 0;
        while (i++ <= root->node_countKeys)
        {
            scanTree(root->totalPointers[i], output);
        }
    }

    return 0;
}


// Creates a serialized string representation of the B-tree, starting from the root node.
// If the tree root is NULL, returns NULL.
char *printBTree(BTreeHandle *tree)
{
    if (treeRoot != NULL)
    {
        node_index = 0; // Reset node index counter before traversal

        int length = DFS(treeRoot); // Traverse the tree to determine required string length

        // Allocate memory for the output string based on the calculated length
        char *output = malloc(length * sizeof(char));

        // Populate `output` with the serialized representation of the tree
        scanTree(treeRoot, output);

        return output; // Return the resulting serialized string
    }
    else
    {
        return NULL; // Return NULL if the tree has no root
    }
}


// Compare two strings and return the result.
// Returns 0 if equal, non-zero if different.
int compareSTR(char *str1, char *str2)
{
    return strcmp(str1, str2);
}

// Get the total number of keys in a leaf node.
int getLeafKeys(Tree_Node *node)
{
    return node->node_countKeys;
}

// Free memory allocated for a Record Identifier (RID).
void freeRID(RID **rid)
{
    free(rid);
}

// Free memory for a tree node value.
void freeTreeNode(Value *node)
{
    free(node);
}

// Retrieve management data from a B-tree scan handle.
Tree_Scan *accessMgmt(BT_ScanHandle *scanHandle)
{
    return scanHandle->mgmtData;
}

// Access management data from a B-tree handle.
BTreeHandle *getTreeData(BTreeHandle *tree)
{
    return tree->mgmtData;
}

// Allocate memory for a Tree_Scan structure and return a pointer to it.
Tree_Scan *allocateTSMem()
{
    return malloc(sizeof(Tree_Scan));
}

// Allocate memory for an array of BT_ScanHandle structures and return a pointer.
BT_ScanHandle *allocateBTMem()
{
    return calloc(4096, sizeof(BT_ScanHandle));
}



// Inserts a new key and the pointer to the right child node at a specific position in a tree node
// Moves existing keys and pointers to the right to create space for the new key and pointer
RC insertIntoNode(Tree_Node *node, int position, Value newKey, Tree_Node *rightChild)
{
    // Initialize index to the current number of keys in the node
    int currentIndex = node->node_countKeys;

    // Check if keys need to be shifted for insertion at the desired position
    if (currentIndex > position)
    {
        // Begin shifting keys and pointers to the right to create space
        while (currentIndex > position)
        {
            // Confirm the node and its components are accessible
            if (node != NULL && node->keys != NULL && node->totalPointers != NULL)
            {
                // Shift key elements to the right
                if (currentIndex > 0)
                {
                    node->keys[currentIndex] = node->keys[currentIndex - 1];
                }

                // Shift pointer elements to the right, within bounds
                if (currentIndex < MAX_KEYS - 1)
                {
                    node->totalPointers[currentIndex + 1] = node->totalPointers[currentIndex];
                }
            }
            currentIndex--;
        }
    }

    // Place the new key and pointer at the specified position
    node->keys[position] = newKey;
    node->totalPointers[position + 1] = rightChild;

    // Safely increment the key count for the node if the node exists
    if (node != NULL)
    {
        node->node_countKeys++;
    }

    return RC_OK; // Indicate successful insertion
}

// Inserts a new key and adjusts pointers in the parent node following a node split
// Adds the key and links the split nodes; creates a new parent if no parent exists
RC insertParent(Tree_Node *leftChild, Tree_Node *rightChild, Value newKey)
{
    // Retrieve the parent node of the left child, if it exists
    Tree_Node *parentNode = (Tree_Node *)leftChild->nodePointer;
    int positionIndex = 0;  // Initialize the index to find left child's position in parent

    // Create a new parent if no parent node exists
    if (parentNode == NULL)
    {
        return createParent(leftChild, rightChild, newKey);
    }
    else
    {
        // Locate the index of the left child within the parent's pointer array
        while (positionIndex < parentNode->node_countKeys)
        {
            // Break if left child's position is found
            if (parentNode->totalPointers[positionIndex] == leftChild)
            {
                break;
            }
            positionIndex++;
        }

        // Choose to insert or split based on the parent's available capacity
        bool hasSpace = parentNode->node_countKeys < reclist_size - 1;
        if (hasSpace)
        {
            // Insert the new key at the identified position if there is available space
            return insertIntoNode(parentNode, positionIndex, newKey, rightChild);
        }
        else
        {
            // Split the parent node if it has reached maximum capacity
            return splitNode(parentNode, positionIndex, newKey, rightChild, reclist_size);
        }
    }
}

// Copies pointers to a new node during a split operation
// Transfers existing pointers and inserts a new pointer at the specified split location
void copyNodes(Tree_Node **newNodePointers, Tree_Node **existingPointers, int maxSize, int insertPosition, Tree_Node *newRightNode)
{
    int newIndex = 0;  // Initialize the index for the new node's pointer array
    bool insertedNewNode = false; // Flag to track insertion of the new node

    // Iterate over each position, copying pointers to the new node
    for (int i = 0; i < maxSize + 1; i++)
    {
        // Insert new node pointer at the designated position; otherwise, copy from original
        if (i == insertPosition + 1 && !insertedNewNode)
        {
            newNodePointers[newIndex++] = newRightNode;
            insertedNewNode = true;
        }
        else
        {
            newNodePointers[newIndex++] = (i <= insertPosition) ? existingPointers[i] : existingPointers[i - 1];
        }
    }
}

// Copies keys to a new array as part of a node split process
// Inserts a new key at a specified position while transferring other keys
void copyKeys(Value *newKeyArray, Value *existingKeys, int maxSize, int insertPosition, Value newKey)
{
    int newArrayIndex = 0;  // Index for new key array
    bool newKeyInserted = false;  // Flag to check if the new key has been inserted

    // Iterate through the existing keys array
    for (int i = 0; i < maxSize; i++)
    {
        // Place new key at specified position; otherwise, copy from original keys
        if (i == insertPosition && !newKeyInserted)
        {
            newKeyArray[newArrayIndex++] = newKey;
            newKeyInserted = true;
        }
        else
        {
            // Copy the key from existing array, shifting if necessary after insertion
            newKeyArray[newArrayIndex++] = (i < insertPosition) ? existingKeys[i] : existingKeys[i - 1];
        }
    }
}

// Divides a node's keys and pointers into separate arrays for reorganization during a node split
// Uses helper functions to transfer pointers and keys into temporary arrays for splitting
void splitNodeIntoArrays(Tree_Node *currentNode, Tree_Node *newRightNode, int insertIndex, Value newKey, int maxArraySize, Tree_Node **tempPointerArray, Value *tempKeyArray)
{
    // Adjust indices and insert the new key and pointer at specified split location
    // Helper functions manage the actual copying into temporary arrays

    // Copy node pointers to the temporary pointer array, including the new pointer
    copyNodes(tempPointerArray, (Tree_Node **)currentNode->totalPointers, maxArraySize, insertIndex, newRightNode);

    // Copy node keys to the temporary key array, inserting the new key at the required position
    copyKeys(tempKeyArray, currentNode->keys, maxArraySize, insertIndex, newKey);
}

// Transfers a subset of pointers from a temporary array into the current node during a split operation
// Updates the original node’s pointers with the first half of entries from the temporary array, up to the specified midpoint
void copyNodePointers(Tree_Node *targetNode, Tree_Node **tempPointerArray, int midpoint)
{
    // Loop through the specified midpoint to assign pointers from the temporary array to the target node
    for (int index = 0; index <= midpoint; index++)
    {
        // Populate the target node's pointers with entries from the temporary array
        targetNode->totalPointers[index] = tempPointerArray[index];
    }
}

// Transfers a subset of keys from a temporary array back to the target node following a split operation
// Updates the original node’s keys with the first half of entries from the temporary key array, up to the designated midpoint
void copyNodeKeys(Tree_Node *targetNode, Value *tempKeyArray, int midpoint)
{
    // Iterate through keys up to the specified midpoint, populating the target node's keys from the temporary array
    for (int index = 0; index < midpoint; index++)
    {
        // Transfer each key from the temporary array into the target node’s keys array
        targetNode->keys[index] = tempKeyArray[index];
    }
}

// Function to handle the splitting of a node in a B-tree when it exceeds the maximum allowed number of keys
RC splitNode(Tree_Node *nodePointer, int scanPosition, Value key, Tree_Node *right, int reclist_size)
{
    // Declare temporary variables for node splitting process
    Tree_Node **temporaryPointers = NULL;  // Array to store pointers during split
    Tree_Node *newNode = NULL;             // Pointer to the newly created right node
    Value *temporaryKeys = NULL;           // Array to store keys during split
    int midNode = 0;                       // Midpoint of node for the split

    // Allocate memory for temporary node pointers; exit on failure
    temporaryPointers = (Tree_Node **)malloc((reclist_size + 1) * sizeof(Tree_Node *));
    if (!temporaryPointers)
    {
        exit(EXIT_FAILURE); // Exit if memory allocation for pointers fails
    }

    // Allocate memory for temporary key storage; exit on failure
    temporaryKeys = (Value *)malloc(reclist_size * sizeof(Value));
    if (!temporaryKeys)
    {
        exit(EXIT_FAILURE); // Exit if memory allocation for keys fails
    }

    // If memory allocation was successful for both arrays
    if (temporaryPointers != NULL && temporaryKeys != NULL)
    {
        // Copy the existing node's keys and pointers into temporary arrays
        // Simultaneously include the new key and pointer for the right node
        splitNodeIntoArrays(nodePointer, right, scanPosition, key, reclist_size, temporaryPointers, temporaryKeys);

        // Compute the middle location for the split based on the total number of keys
        midNode = (reclist_size % 2 == 0) ? (reclist_size / 2) : (reclist_size / 2 + 1);

        // Adjust the original node's key count to reflect the left half of the split
        nodePointer->node_countKeys = midNode - 1;

        // Update the original node with the first half of the pointers and keys
        copyNodePointers(nodePointer, temporaryPointers, midNode);
        copyNodeKeys(nodePointer, temporaryKeys, midNode);

        // Create a new node to hold the second half of the keys and pointers
        newNode = createNode();
        if (newNode) // If new node creation is successful
        {
            // Set the new node's key count based on the split
            newNode->node_countKeys = reclist_size - midNode;
            int index = 0;
            int i = midNode;

            // Transfer the remaining keys and pointers into the new node
            while (i <= reclist_size)
            {
                Tree_Node *currentNode = temporaryPointers[i];
                Value currentKey = temporaryKeys[i];

                newNode->totalPointers[index] = currentNode;
                newNode->keys[index++] = currentKey;

                i++;
            }

            // Set the parent pointer for the new node to be the same as the original node's parent
            newNode->nodePointer = nodePointer->nodePointer;

            // The middle key that will be pushed up to the parent node
            Value middleKey = temporaryKeys[midNode - 1];

            // Clean up the temporary arrays
            free(temporaryPointers);
            free(temporaryKeys);

            // Insert the middle key and the new node into the parent node
            return insertParent(nodePointer, newNode, middleKey);
        }
        else // If memory allocation for the new node failed
        {
            free(temporaryPointers);
            free(temporaryKeys);

            return RC_IM_MEMORY_ERROR; // Return memory error
        }
    }
    else // Handle memory allocation failure for the temporary arrays
    {
        free(temporaryPointers);
        free(temporaryKeys);
        return RC_IM_MEMORY_ERROR; // Return memory error
    }
}


///riddhi//////////



// Function to remove a specified key and its corresponding pointer from a B-tree node
void removeKeyAndPointer(Tree_Node *bNodeTree, int scanPosition, int NumKeys, bool isLeaf)
{
    // Determine the starting index for removal; 
    // for internal nodes, we start one position before the scanPosition
    int btreeIndex = isLeaf ? scanPosition : scanPosition - 1;

    // Set limit as the current total number of keys to handle boundary adjustments
    int limit = NumKeys;

    // Special handling for leaf nodes to manage memory
    if (isLeaf)
    {
        // Ensure the node and pointer at scanPosition are valid before freeing memory
        if (bNodeTree != NULL && bNodeTree->totalPointers != NULL && bNodeTree->totalPointers[scanPosition] != NULL)
        {
            free(bNodeTree->totalPointers[scanPosition]);  // Free the pointer memory at scanPosition
            bNodeTree->totalPointers[scanPosition] = NULL; // Clear the pointer to avoid dangling references
        }
    }

    // Loop to shift all keys and pointers left to fill the gap left by the removed key
    for (int i = btreeIndex; i < limit; i++)
    {
        // Shift keys to fill the removed key's position
        bNodeTree->keys[i] = bNodeTree->keys[i + 1];

        // Handle pointers based on whether the node is a leaf or internal
        if (isLeaf)
        {
            // For leaf nodes, shift pointers directly to the left
            if (bNodeTree->totalPointers[i + 1] != NULL)
            {
                bNodeTree->totalPointers[i] = bNodeTree->totalPointers[i + 1];
            }
        }
        else
        {
            // For internal nodes, shift child pointers accordingly
            // Only shift if within valid range (i < limit - 1) for internal nodes
            if (i < limit - 1)
            {
                bNodeTree->totalPointers[i + 1] = bNodeTree->totalPointers[i + 2];
            }
        }
    }

    // Clear the last key and pointer after the shift to prevent dangling data
    bNodeTree->keys[limit] = emptyList; // Reset the last key to a predefined empty state

    // Clear the last pointer position based on node type (leaf or internal)
    if (isLeaf)
    {
        bNodeTree->totalPointers[limit] = NULL;  // Clear the last pointer in a leaf node
    }
    else
    {
        bNodeTree->totalPointers[limit + 1] = NULL;  // Clear the extra pointer in an internal node
    }
}


// Function to handle underflow in the root of a B-tree
// This function is called when the root node becomes empty after a deletion
// It may result in reducing the height of the tree by promoting a child node
RC handleRootUnderflow(Tree_Node **treeRootPtr, int totalList)
{
    // Get direct access to the root node through dereferencing
    // This allows us to modify the actual root node, not just a copy
    Tree_Node *treeRoot = *treeRootPtr; // Dereference to get the current root of the tree

    // First check: Verify if root actually needs handling
    // If root has any keys, there's no underflow condition
    if (treeRoot->node_countKeys > 0)
        return RC_OK; // Return OK if there is no underflow

    // Initialize pointer for potential new root
    // This will hold the child node that might become the new root
    Tree_Node *newtreeRoot = NULL; // Variable to hold the new root if the current root is emptied

    // Handle different cases based on whether root is a leaf or internal node
    // Internal nodes can have their children promoted, leaf nodes cannot
    if (!treeRoot->Node_Leaf)
    {
        // For internal nodes: Check if there's a valid first child to promote
        // The first child (at index 0) becomes the new root
        if (treeRoot->totalPointers[0] != NULL)
        {
            newtreeRoot = treeRoot->totalPointers[0];
            // Update the parent pointer of the new root
            // Since it's becoming the root, it should have no parent
            newtreeRoot->nodePointer = NULL;
        }
        else
        {
            // Error case: Internal node has no valid child to promote
            // This is an inconsistent state that shouldn't occur in a valid B-tree
            return RC_ERROR;
        }
    }
    else
    {
        // Error case: Leaf root node with underflow
        // This is typically an invalid state as the root leaf should maintain at least one key
        return RC_ERROR;
    }

    // Cleanup phase: Prepare to free all resources associated with old root
    // Create array of pointers to all resources that need to be freed
    void **resourcesToFree = (void *[]){treeRoot->keys, treeRoot->totalPointers, treeRoot};
    int i;

    // Systematically free all resources in the array
    // This prevents memory leaks by properly deallocating all allocated memory
    for (i = 0; i < 3; i++)
    {
        free(resourcesToFree[i]); // Free the memory for keys, pointers, and the tree node itself
    }

    // Nullify pointers after freeing to prevent dangling references
    // This is good practice to avoid potential use-after-free errors
    treeRoot->keys = NULL;
    treeRoot->totalPointers = NULL;

    // Update the tree structure
    // Point the root pointer to the new root node
    *treeRootPtr = newtreeRoot;
    totalList -= 1; // Decrement the count of total nodes in the list

    // Operation completed successfully
    // The tree height has been reduced by one level
    return RC_OK; // Return OK to indicate the root underflow has been successfully handled
}




RC mergeNodes(Tree_Node *parentNode, Tree_Node *bNodeTree, Tree_Node *brother, 
              int nodePosition, int NumKeys, int reclist_size, int totalList) {
    // Index to keep track of position in brother node during merging
    int index = brother->node_countKeys;
    // Index for traversing source node (bNodeTree)
    int dst = 0;

    // If merging with first node, swap bNodeTree and brother, and adjust position
    nodePosition = (nodePosition == 0) ? (bNodeTree = brother, brother = bNodeTree, 1) : nodePosition;
    
    // Adjust key count based on node position after potential swap
    NumKeys = (nodePosition == 1) ? bNodeTree->node_countKeys : NumKeys;

    // Special handling for internal nodes vs leaf nodes
    if (!bNodeTree->Node_Leaf) {
        // For internal nodes, move parent's separator key to brother node
        brother->keys[index] = parentNode->keys[nodePosition - 1];
        index = index + 1;
        NumKeys = NumKeys + 1;
    } else {
        // Leaf nodes should not reach this point in normal operation
        return RC_ERROR;
    }

    // Transfer all keys and pointers from bNodeTree to brother node
    while (dst < NumKeys) {
        // Safety check for brother node
        if (brother != NULL) {
            // Copy keys and pointers from bNodeTree to brother
            brother->keys[index] = bNodeTree->keys[dst];
            brother->totalPointers[index] = bNodeTree->totalPointers[dst];
        } else {
            return RC_ERROR;
        }

        // Safety check for bNodeTree
        if (bNodeTree != NULL) {
            // Clean up source node after transfer
            bNodeTree->keys[dst] = emptyList;
            bNodeTree->totalPointers[dst] = NULL;
        } else {
            return RC_ERROR;
        }

        // Move to next positions in both nodes
        index = index + 1;
        dst = dst + 1;
    }

    // Update brother node's key count after merging
    brother->node_countKeys += NumKeys;

    // Handle the last pointer (important for maintaining tree structure)
    Tree_Node *lastPointer = bNodeTree->totalPointers[reclist_size - 1];
    brother->totalPointers[reclist_size - 1] = lastPointer;

    // Update total node count in the tree
    totalList -= 1;

    // Clean up the merged node (bNodeTree)
    void **pointersToFree = (void *[]){
        bNodeTree->keys,
        bNodeTree->totalPointers,
        bNodeTree
    };
    void **pointersToNullify = (void *[]){
        &(bNodeTree->keys),
        &(bNodeTree->totalPointers),
        &bNodeTree
    };

    // Free memory and nullify pointers
    for (int i = 0; i < 3; i++) {
        free(pointersToFree[i]);
        *(void **)pointersToNullify[i] = NULL;
    }

    // Remove the merged node reference from parent
    return deleteNode(parentNode, nodePosition);
}

void shiftNodeContents(Tree_Node *bNodeTree, Tree_Node *brother, 
                      Tree_Node *parentNode, int nodePosition, int NumKeys) {
    int shiftIndex, broKey;

    // Control value determines shift behavior:
    // Bit 0: Set if nodePosition != 0
    // Bit 1: Set if node is internal (not leaf)
    int control = (nodePosition != 0) | ((!bNodeTree->Node_Leaf) << 1);

    switch (control) {
        case 1:  // Right shift in leaf node
        case 3:  // Right shift in internal node
        {
            // Determine which key to borrow based on node type
            int broKey = (control == 3) ? 
                        brother->node_countKeys - 1 : brother->node_countKeys;

            // Prepare space for new elements
            bNodeTree->totalPointers[NumKeys + 1] = bNodeTree->totalPointers[NumKeys];

            // Shift existing elements right
            int shiftIndex = NumKeys;
            do {
                if (bNodeTree != NULL && bNodeTree->keys != NULL && 
                    bNodeTree->totalPointers != NULL) {
                    if (shiftIndex >= 1) {
                        bNodeTree->keys[shiftIndex] = bNodeTree->keys[shiftIndex - 1];
                        bNodeTree->totalPointers[shiftIndex] = 
                            bNodeTree->totalPointers[shiftIndex - 1];
                    }
                }
                shiftIndex--;
            } while (shiftIndex > 0);

            // Insert borrowed key based on node type
            if (control == 3) {
                bNodeTree->keys[0] = parentNode->keys[nodePosition - 1];
            } else {
                bNodeTree->keys[0] = brother->keys[broKey];
            }

            // Update parent's key if necessary
            if (parentNode != NULL && parentNode->keys != NULL && nodePosition > 0) {
                parentNode->keys[nodePosition - 1] = brother->keys[broKey];
            }

            // Move pointer from brother to bNodeTree
            if (bNodeTree != NULL && bNodeTree->totalPointers != NULL) {
                bNodeTree->totalPointers[0] = brother->totalPointers[broKey];
            }

            // Clean up brother node after borrowing
            if (brother != NULL && brother->keys != NULL && 
                brother->totalPointers != NULL) {
                brother->keys[broKey] = emptyList;
                brother->totalPointers[broKey] = NULL;
            }
        }
        break;

        case 0:  // Left shift in leaf node
        case 2:  // Left shift in internal node
        {
            if (bNodeTree != NULL && parentNode != NULL && brother != NULL) {
                int temp = brother->node_countKeys;
                Tree_Node *sourceNode;
                Value sourceKey;
                Tree_Node *sourcePointer;

                // Set up source based on node type
                if (control == 2) {
                    sourceNode = brother;
                    sourceKey = parentNode->keys[0];
                    sourcePointer = brother->totalPointers[0];
                } else {
                    sourceNode = bNodeTree;
                    sourceKey = brother->keys[0];
                    sourcePointer = bNodeTree->totalPointers[NumKeys];
                }

                // Move elements to target position
                if (sourceNode != NULL) {
                    bNodeTree->keys[NumKeys] = sourceKey;
                    bNodeTree->totalPointers[NumKeys] = sourcePointer;
                }

                // Update parent key for internal nodes
                if (control == 2) {
                    parentNode->keys[0] = sourceKey;
                }

                // Shift remaining elements left in brother node
                int shiftIndex = 0;
                do {
                    if (brother->keys != NULL && brother->totalPointers != NULL && 
                        shiftIndex < temp) {
                        brother->keys[shiftIndex] = brother->keys[shiftIndex + 1];
                        brother->totalPointers[shiftIndex] = 
                            brother->totalPointers[shiftIndex + 1];
                    }
                    shiftIndex++;
                } while (shiftIndex < temp);

                // Clean up last positions in brother node
                if (brother->keys != NULL && brother->totalPointers != NULL) {
                    brother->totalPointers[temp] = NULL;
                    brother->keys[temp] = emptyList;
                }
            }
        }
        break;
    }
}

// Function to adjust nodes after a deletion operation in a B-tree
// This function handles the redistribution of keys between sibling nodes
// to maintain B-tree properties after a deletion operation
RC adjustNodesAfterDeletion(Tree_Node *parentNode, Tree_Node *bNodeTree, Tree_Node *brother, int nodePosition, int NumKeys)
{
    // Call shiftNodeContents to redistribute keys and pointers between nodes
    // This ensures proper balance between siblings after deletion
    shiftNodeContents(bNodeTree, brother, parentNode, nodePosition, NumKeys);

    // Update the key counts in both nodes after redistribution
    // The current node gains one key while the brother node loses one
    bNodeTree->node_countKeys++;    // Increment current node's key count
    brother->node_countKeys--;      // Decrement brother node's key count

    // Return success status
    // Note: The conditional check is redundant as it always returns RC_OK
    // Could be simplified to just return RC_OK
    return (bNodeTree != NULL) ? RC_OK : RC_OK; // The check is redundant; always returns RC_OK
}

// Function to delete a node from a B-tree
// Handles the removal of a key and associated pointer at a specified position
RC deleteNode(Tree_Node *bNodeTree, int scanPosition)
{
    // Declare variables for iteration (currently unused)
    int i;                                     // Loop variable for general use, not used in current scope
    int j;                                     // Loop variable for general use, not used in current scope
    
    // Initialize sibling node pointer (currently unused in reachable code)
    Tree_Node *brother = NULL;                 // Variable to hold reference to a sibling node, currently uninitialized
    
    // Decrement and store the new key count
    int NumKeys = --bNodeTree->node_countKeys; // Decrement the node's key count and store the new count

    // Remove the specified key and its associated pointer
    // The leaf parameter determines how the removal is handled
    removeKeyAndPointer(bNodeTree, scanPosition, NumKeys, bNodeTree->Node_Leaf);

    // Current implementation returns here
    // All code below this point is unreachable
    return RC_OK;

    // UNREACHABLE CODE BELOW THIS POINT
    // The following code represents the original logic for handling underflow
    // but is currently unreachable due to the return statement above

    // Calculate minimum keys required to avoid underflow
    int splitSize = (bNodeTree->Node_Leaf) ? reclist_size / 2 : (reclist_size - 1) / 2;
    return (NumKeys >= splitSize) ? RC_OK : (bNodeTree == treeRoot) ? handleRootUnderflow(&treeRoot, totalList)
                                                                    : RC_CONTINUE;

    // Find the node's position within its parent
    Tree_Node *parentNode = (Tree_Node *)bNodeTree->nodePointer;
    int nodePosition = 0;

    // Search for the node's position in parent's pointers
    for (; nodePosition < parentNode->node_countKeys; nodePosition++)
    {
        if (parentNode->totalPointers[nodePosition] == bNodeTree)
        {
            break;
        }
    }

    // Check if merging with sibling is possible
    int brotherSize = (bNodeTree->Node_Leaf) ? reclist_size - 1 : reclist_size - 2;
    if (brother->node_countKeys + NumKeys <= brotherSize)
    {
        return mergeNodes(parentNode, bNodeTree, brother, nodePosition, NumKeys, reclist_size, totalList);
    }

    // Handle redistribution if merging isn't necessary
    if (NumKeys >= 0)
    {
        return adjustNodesAfterDeletion(parentNode, bNodeTree, brother, nodePosition, NumKeys);
    }
    else
    {
        return RC_ERROR; // Return error if there is an inconsistency in key count (unreachable code)
    }
}


// Establishes a new parent node that connects two child nodes, assigning a middle key
RC createParent(Tree_Node *leftChild, Tree_Node *rightChild, Value middleKey)
{
    // Attempt to create the parent node to serve as a new root for the given child nodes
    Tree_Node *newParentNode = createNode();
    if (newParentNode == NULL)
    {
        return RC_IM_MEMORY_ERROR; // Return error if memory allocation fails
    }

    // Temporarily store child nodes for easy pointer assignment within the parent node
    Tree_Node *childNodes[2] = {leftChild, rightChild}; 
    newParentNode->node_countKeys = 1;  // Set key count to 1 as the parent will initially contain one key

    // Loop through child nodes array to set key and pointers in the parent node
    int childIndex = 0;
    while (childIndex < 2)
    {
        if (childIndex == 0)
        {
            newParentNode->keys[childIndex] = middleKey; // Assign the middle key to the new parent node
        }

        // Assign left and right pointers to the new parent node
        newParentNode->totalPointers[childIndex] = childNodes[childIndex];
        
        // Update the parent pointer for each child to refer to the new parent node
        childNodes[childIndex]->nodePointer = (struct NodeTree *)newParentNode;
        
        childIndex++;
    }

    // Set the new parent node as the tree root in the global scope
    treeRoot = newParentNode;
    return RC_OK; // Indicate successful creation of the parent node
}



