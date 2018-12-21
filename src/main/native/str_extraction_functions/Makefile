include ../Makefile.common

.PHONY: libextraction.so clean


all: libextraction.so

extraction:
	if [ ! -d "$(BUILD_DIR_EXTRACTION)" ]; then mkdir $(BUILD_DIR_EXTRACTION); fi
	$(CC) $(EXTRACTION_FLAGS) str_extraction_functions.cpp -o $(BUILD_DIR_EXTRACTION)/str_extraction_functions.o $(LIBEXTRACTION_LIBS)

libextraction.so: extraction
	$(CC) $(LIBEXTRACTION_FLAGS) $(BUILD_DIR_EXTRACTION)/libextraction.so $(BUILD_DIR_EXTRACTION)/*.o
	cp $(BUILD_DIR_EXTRACTION)/libextraction.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_EXTRACTION)