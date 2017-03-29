include ./Makefile.common

.PHONY: libmetacache.so metacache libfunctions.so functions clean

all: libmetacache.so libfunctions.so
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

libmetacache.so: metacache
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR_HASHMAP)/libmetacache.so $(BUILD_DIR_HASHMAP)/*.o $(LIBMETACACHE_LIBS)
	cp $(BUILD_DIR_HASHMAP)/libmetacache.so ../../../target/classes/

libfunctions.so: functions
	$(GCC) $(LIBFUNCTIONS_FLAGS) $(BUILD_DIR_FUNCTIONS)/libfunctions.so $(BUILD_DIR_FUNCTIONS)/*.o $(LIBFUNCTIONS_LIBS)
	cp $(BUILD_DIR_FUNCTIONS)/libfunctions.so ../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_HASHMAP)
	$(RMR) $(BUILD_DIR_FUNCTIONS)
	#$(RM) ../resources/libmetacache.so
