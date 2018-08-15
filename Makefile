objects = buffer_mgr.o buffer_mgr_stat.o dberror.o expr.o rm_serializer.o storage_mgr.o record_mgr.o

test_assign: $(objects) test_expr.o test_assign3_1.o 
	cc -o test_expr $(objects) test_expr.o
	cc -o test_assign3 $(objects) test_assign3_1.o
	

dberror.o: dberror.h
storage_mgr.o: storage_mgr.h dberror.h
buffer_mgr_stat.o: buffer_mgr_stat.h buffer_mgr.h
buffer_mgr.o: buffer_mgr.h storage_mgr.h dberror.h
record_mgr.o: dberror.h expr.h tables.h buffer_mgr.h storage_mgr.h record_mgr.h
rm_serializer.o: dberror.h tables.h record_mgr.h
expr.o: expr.h dberror.h tables.h record_mgr.h
test_expr.o: test_helper.h expr.h dberror.h tables.h record_mgr.h
test_assign3_1.o: test_helper.h expr.h dberror.h tables.h record_mgr.h


clean:
	rm test_assign3 test_expr *.o
