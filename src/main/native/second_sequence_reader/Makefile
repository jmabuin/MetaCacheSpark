include ../Makefile.common

.PHONY: libsequencereader2.so clean


all: libsequencereader2.so

sequencereader:
	if [ ! -d "$(BUILD_DIR_SEQUENCEREADER)" ]; then mkdir $(BUILD_DIR_SEQUENCEREADER); fi
	$(CC) $(METACACHE_FLAGS) sequence_reader.cpp -o $(BUILD_DIR_SEQUENCEREADER)/sequence_reader.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) fasta_reader.cpp -o $(BUILD_DIR_SEQUENCEREADER)/fasta_reader.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) fastq_reader.cpp -o $(BUILD_DIR_SEQUENCEREADER)/fastq_reader.o $(LIBMETACACHE_LIBS)
	$(CC) $(METACACHE_FLAGS) fasta_file_reader_native_functions.cpp -o $(BUILD_DIR_SEQUENCEREADER)/fasta_file_reader_native_functions.o $(LIBMETACACHE_LIBS)

libsequencereader2.so: sequencereader
	$(CC) $(LIBMETACACHE_FLAGS) $(BUILD_DIR_SEQUENCEREADER)/libsequencereader2.so $(BUILD_DIR_SEQUENCEREADER)/*.o
	cp $(BUILD_DIR_SEQUENCEREADER)/libsequencereader2.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_SEQUENCEREADER)