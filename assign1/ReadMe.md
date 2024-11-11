# Assignment 1 - Storage Manager Implementation

# Group 44

## Members:

### Sanketkumar Patel (A20523237)
Responsible for file handling and block manipulation functions.
Implemented functions:

readLastBlock,
writeBlock,
writeCurrentBlock,
appendEmptyBlock,
ensureCapacity.

### Hetanshi Deepakkumar Darbar (A20552832)
Responsible for retrieving and reading specific blocks from the file.
Implemented functions:

getBlockPos,
readFirstBlock,
readPreviousBlock,
readCurrentBlock,
readNextBlock.

### Riddhi Das (A20582829)
Responsible for file creation, opening, and basic file operations.
Implemented functions:

initStorageManager,
createPageFile,
openPageFile,
closePageFile,
destroyPageFile,
readBlock.

# Steps to follow to run the code

## 1. Clone the Repository:
Clone the project repository from GitHub using followin command. Replace <project_directory> to desired folder where you want to clone the project repository:

git clone https://github.com/CS525-ADO-F24/CS525-F24-G44.git

cd <project_directory>


## 2. Clean the Build
To remove all generated object files and executables before executing project, run following command in the terminal:

make clean


## 3. To Compile the Project
To compile the project, run the following command in the terminal: (This will generate the executable test_assign1)

make


## 4. To run the test cases
To run the compiled program, use following command in the terminal:

./test_assign1


## 5. Clean the Build (Repeating step2)
To remove all generated object files and executables, run following command in the terminal:

make clean