CC=gcc
CFLAGS=-I.
DEPS = btree_mgr.h buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h expr.h record_mgr.h storage_mgr.h tables.h test_helper.h
OBJ = btree_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

all: test_expr test_assign4_1 test_assign4_2

test_assign4_1.o: test_assign4_1.c
	$(CC) -c test_assign4_1.c

test_assign4_1: $(OBJ) test_assign4_1.o
	gcc -o $@ $^ $(CFLAGS)

test_expr.o: test_expr.c
	$(CC) -c test_expr.c

test_expr: $(OBJ) test_expr.o
	gcc -o $@ $^ $(CFLAGS)

test_assign4_2.o: test_assign4_2.c
	$(CC) -c test_assign4_2.c

test_assign4_2: $(OBJ) test_assign4_2.o
	gcc -o $@ $^ $(CFLAGS)

dberror.o: dberror.c dberror.h
	$(CC) -c dberror.c

record_mgr.o: record_mgr.c record_mgr.h tables.h buffer_mgr.h storage_mgr.h
	$(CC) -c record_mgr.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dberror.h storage_mgr.h
	$(CC) -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) -c buffer_mgr_stat.c

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	$(CC) -c storage_mgr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	$(CC) -c rm_serializer.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	$(CC) -c expr.c

btree_helper.o: btree_helper.c btree_helper.h
	$(CC) -c btree_helper.c


run1:
	./test_assign4_1 

run2:
	./test_assign4_2 

run3:
	./test_expr

.PHONY : clean
clean:
	rm -f *.o test_assign4_1 test_expr test_assign4_2
