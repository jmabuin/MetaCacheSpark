include ./Makefile.common

.PHONY: libmetacache.so metacache libfunctions.so functions libmymap.so libsequencereader.so mymap clean

all: libmetacache.so libfunctions.so libmymap.so libsequencereader.so
	@echo "================================================================================"
	@echo "libmetacache library has been built."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "================================================================================"

metacache:
	if [ ! -d "$(BUILD_DIR_HASHMAP)" ]; then mkdir $(BUILD_DIR_HASHMAP); fi
	$(CC) $(METACACHE_FLAGS) chunck.cpp -o $(BUILD_DIR_HASHMAP)/chunck.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) chunck_allocator.cpp -o $(BUILD_DIR_HASHMAP)/chunck_allocator.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) bucket.cpp -o $(BUILD_DIR_HASHMAP)/bucket.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) hashmultimap.cpp -o $(BUILD_DIR_HASHMAP)/hashmultimap.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) nativefunctions.cpp -o $(BUILD_DIR_HASHMAP)/nativefunctions.o $(LIBMETACACHE_LIBS)

functions:
	if [ ! -d "$(BUILD_DIR_FUNCTIONS)" ]; then mkdir $(BUILD_DIR_FUNCTIONS); fi
	$(GCC) $(FUNCTIONS_FLAGS) MetaCacheFunctions.c -o $(BUILD_DIR_FUNCTIONS)/MetaCacheFunctions.o $(LIBFUNCTIONS_LIBS)

mymap:
	if [ ! -d "$(BUILD_DIR_MYMAP)" ]; then mkdir $(BUILD_DIR_MYMAP); fi
	$(CC) $(MYMAP_FLAGS) mapnative.cpp -o $(BUILD_DIR_MYMAP)/mapnative.o $(LIBMYMAP_LIBS)

libmetacache.so: metacache
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR_HASHMAP)/libmetacache.so $(BUILD_DIR_HASHMAP)/*.o $(LIBMETACACHE_LIBS)
	cp $(BUILD_DIR_HASHMAP)/libmetacache.so ../../../target/classes/

libfunctions.so: functions
	$(GCC) $(LIBFUNCTIONS_FLAGS) $(BUILD_DIR_FUNCTIONS)/libfunctions.so $(BUILD_DIR_FUNCTIONS)/*.o $(LIBFUNCTIONS_LIBS)
	cp $(BUILD_DIR_FUNCTIONS)/libfunctions.so ../../../target/classes/

libmymap.so: mymap
	$(CC) $(LIBMYMAP_FLAGS) $(BUILD_DIR_MYMAP)/libmymap.so $(BUILD_DIR_MYMAP)/*.o $(LIBMYMAP_LIBS)
	cp $(BUILD_DIR_MYMAP)/libmymap.so ../../../target/classes/

sequencereader:
	if [ ! -d "$(BUILD_DIR_SEQUENCEREADER)" ]; then mkdir $(BUILD_DIR_SEQUENCEREADER); fi
	$(CC) $(METACACHE_FLAGS) sequence_reader.cpp -o $(BUILD_DIR_SEQUENCEREADER)/sequence_reader.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) fasta_reader.cpp -o $(BUILD_DIR_SEQUENCEREADER)/fasta_reader.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) fasta_file_reader_native_functions.cpp -o $(BUILD_DIR_SEQUENCEREADER)/fasta_file_reader_native_functions.o $(LIBMETACACHE_LIBS)

libsequencereader.so: sequencereader
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR_SEQUENCEREADER)/libsequencereader.so $(BUILD_DIR_SEQUENCEREADER)/*.o
	cp $(BUILD_DIR_SEQUENCEREADER)/libsequencereader.so ../../../target/classes/


clean:
	$(RMR) $(BUILD_DIR_HASHMAP)
	$(RMR) $(BUILD_DIR_FUNCTIONS)
	$(RMR) $(BUILD_DIR_MYMAP)
	$(RMR) $(BUILD_DIR_SEQUENCEREADER)
	#$(RM) ../resources/libmetacache.so
