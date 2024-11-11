# Compiler and flags
CC = gcc
CFLAGS = -std=c99 -Wall -I.

# Header files
DEPS = dberror.h storage_mgr.h test_helper.h

# Object files for the test
OBJ = dberror.o storage_mgr.o test_assign1_1.o

# Rule for compiling .c files into .o files
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

# Target for the test
test_assign1: $(OBJ)
	$(CC) -o test_assign1 $^ $(CFLAGS)

# Clean up object files and executables
clean:
	rm -f $(OBJ) test_assign1

# We took reference from https://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/