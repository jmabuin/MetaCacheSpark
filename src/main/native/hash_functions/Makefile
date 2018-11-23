include ../Makefile.common

.PHONY: libfunctions.so functions

all: libfunctions.so

functions:
	if [ ! -d "$(BUILD_DIR_FUNCTIONS)" ]; then mkdir $(BUILD_DIR_FUNCTIONS); fi
	$(CC) $(FUNCTIONS_FLAGS) MetaCacheFunctions.c -o $(BUILD_DIR_FUNCTIONS)/MetaCacheFunctions.o $(LIBFUNCTIONS_LIBS)

libfunctions.so: functions
	$(CC) $(LIBFUNCTIONS_FLAGS) $(BUILD_DIR_FUNCTIONS)/libfunctions.so $(BUILD_DIR_FUNCTIONS)/*.o $(LIBFUNCTIONS_LIBS)
	cp $(BUILD_DIR_FUNCTIONS)/libfunctions.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_FUNCTIONS)