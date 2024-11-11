# Assignment 3 - Record Manager Implementation

## Group 44

### Members

- **Sanketkumar Patel (A20523237)**  
  Responsibilities:
  - `initRecordManager(void *mgmtData)`
  - `shutdownRecordManager()`
  - `createTable(char *name, Schema *schema)`
  - `openTable(RM_TableData *rel, char *name)`
  - `closeTable(RM_TableData *rel)`
  - `deleteTable(char *name)`
  - `getNumTuples(RM_TableData *rel)`

- **Hetanshi Deepakkumar Darbar (A20552832)**  
  Responsibilities (Schema-related functions):
  - `getRecordSize(Schema *schema)`
  - `createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)`
  - `freeSchema(Schema *schema)`

  Responsibilities (Record and attribute handling):
  - `createRecord(Record **record, Schema *schema)`
  - `freeRecord(Record *record)`
  - `getAttr(Record *record, Schema *schema, int attrNum, Value **value)`
  - `setAttr(Record *record, Schema *schema, int attrNum, Value *value)`

- **Riddhi Das (A20582829)**  
  Responsibilities (Record management in tables):
  - `insertRecord(RM_TableData *rel, Record *record)`
  - `deleteRecord(RM_TableData *rel, RID id)`
  - `updateRecord(RM_TableData *rel, Record *record)`
  - `getRecord(RM_TableData *rel, RID id, Record *record)`

  Responsibilities (Scan operations):
  - `startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)`
  - `next(RM_ScanHandle *scan, Record *record)`
  - `closeScan(RM_ScanHandle *scan)`

## Instructions for Running the Code

### 1. Clone the Repository and Navigate to the "assign3" Folder
   - Clone the project repository from GitHub and open the folder named `assign3`.

### 2. Clean the Build
   - To remove all generated object files and executables, run:
     ```
     make clean
     ```

### 3. Compile the Project
   - To compile the code, execute:
     ```
     make
     ```

### 4. Run the Test Cases
   - To run the compiled program's test cases, use:
     ```
     make run
     ```

### 5. Clean the Build Again
   - To remove all generated files and executables after testing, repeat:
     ```
     make clean
     ```

### 6. Execute Steps 2-5 in One Command (Optional)
   - To clean, compile, and run the test cases in one go, use:
     ```
     make shortcut
     ```

## Additional Notes

1. To run the `test_assign3_1` test case, use following after step 3:
   ```
   ./test_assign3_1
   ```

2. To run the `test_expr` test case, use following after step 3:
   ```
   ./test_expr
   ```
3. To push files to github use following after step 5 or Step 6:
   ```
   make github
   ```
