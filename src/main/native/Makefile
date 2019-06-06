include ./Makefile.common

.PHONY: libmetacache.so metacache libfunctions.so functions libsequencereader.so libsequencereader2.so libextraction.so mymap clean

all: libmetacache.so libfunctions.so libsequencereader.so libsequencereader2.so libextraction.so
	@echo "================================================================================"
	@echo "Native libraries have been built."
	@echo "Location    = $(LOCATION)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "================================================================================"

libmetacache.so:
	make -C metacache_hashmap

libfunctions.so:
	make -C hash_functions

libsequencereader.so:
	make -C sequence_reader

libsequencereader2.so:
	make -C second_sequence_reader

libextraction.so:
	make -C str_extraction_functions

clean:
	make -C metacache_hashmap clean
	make -C hash_functions clean
	make -C sequence_reader clean
	make -C second_sequence_reader clean
	make -C str_extraction_functions clean

