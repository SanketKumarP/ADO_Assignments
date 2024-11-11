# Compiler and flags
CC = gcc
CFLAGS = -std=c99 -Wall -I.

# Header files
DEPS = dberror.h storage_mgr.h test_helper.h buffer_mgr_stat.h buffer_mgr.h

# Object files for the test
OBJ = dberror.o storage_mgr.o test_assign2_1.o buffer_mgr_stat.o buffer_mgr.o

# Rule for compiling .c files into .o files
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

# Target for the test
test_assign2_1: $(OBJ)
	$(CC) -o test_assign2_1 $^ $(CFLAGS)

# To run the test using Make run
run:
	./test_assign2_1

# To Clean up object files and executables
clean:
	rm -f $(OBJ) test_assign2_1

# We took reference from https://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/