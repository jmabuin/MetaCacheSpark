include ./Makefile.common

.PHONY: libmetacache.so metacache clean

all: libmetacache.so
	@echo "================================================================================"
	@echo "libmetacache library has been built."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "================================================================================"

metacache:
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	$(CC) $(METACACHE_FLAGS) MetaCacheFunctions.c -o $(BUILD_DIR)/MetaCacheFunctions.o $(LIBMETACACHE_LIBS)

libmetacache.so: metacache
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR)/libmetacache.so $(BUILD_DIR)/*.o $(LIBMETACACHE_LIBS)
	cp $(BUILD_DIR)/libmetacache.so ../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR)
	#$(RM) ../resources/libmetacache.so
