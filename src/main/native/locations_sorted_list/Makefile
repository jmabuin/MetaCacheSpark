include ../Makefile.common

.PHONY: liblocationssortedlist.so

all: liblocationssortedlist.so

locationssortedlist:
	if [ ! -d "$(BUILD_DIR_LOCATIONS_SORTED_LIST)" ]; then mkdir $(BUILD_DIR_LOCATIONS_SORTED_LIST); fi
	$(CC) $(LOCATIONS_SORTED_LIST_FLAGS) locations_sorted_list.cpp -o $(BUILD_DIR_LOCATIONS_SORTED_LIST)/locations_sorted_list.o $(LIBLOCATIONS_SORTED_LIST_LIBS)

liblocationssortedlist.so: locationssortedlist
	$(CC) $(LIBLOCATIONS_SORTED_LIST_FLAGS) $(BUILD_DIR_LOCATIONS_SORTED_LIST)/liblocationssortedlist.so $(BUILD_DIR_LOCATIONS_SORTED_LIST)/*.o $(LIBLOCATIONS_SORTED_LIST_LIBS)
	cp $(BUILD_DIR_LOCATIONS_SORTED_LIST)/liblocationssortedlist.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_LOCATIONS_SORTED_LIST)