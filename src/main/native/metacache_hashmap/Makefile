include ../Makefile.common

.PHONY: libmetacache.so metacache

all: libmetacache.so

metacache:
	if [ ! -d "$(BUILD_DIR_HASHMAP)" ]; then mkdir $(BUILD_DIR_HASHMAP); fi
	$(CC) $(METACACHE_FLAGS) chunck.cpp -o $(BUILD_DIR_HASHMAP)/chunck.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) chunck_allocator.cpp -o $(BUILD_DIR_HASHMAP)/chunck_allocator.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) bucket.cpp -o $(BUILD_DIR_HASHMAP)/bucket.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) hashmultimap.cpp -o $(BUILD_DIR_HASHMAP)/hashmultimap.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) nativefunctions.cpp -o $(BUILD_DIR_HASHMAP)/nativefunctions.o $(LIBMETACACHE_LIBS)

libmetacache.so: metacache
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR_HASHMAP)/libmetacache.so $(BUILD_DIR_HASHMAP)/*.o $(LIBMETACACHE_LIBS)
	cp $(BUILD_DIR_HASHMAP)/libmetacache.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_HASHMAP)