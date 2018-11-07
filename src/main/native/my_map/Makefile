include ../Makefile.common

.PHONY: libmymap.so mymap

all: libmymap.so

mymap:
	if [ ! -d "$(BUILD_DIR_MYMAP)" ]; then mkdir $(BUILD_DIR_MYMAP); fi
	$(CC) $(MYMAP_FLAGS) mapnative.cpp -o $(BUILD_DIR_MYMAP)/mapnative.o $(LIBMYMAP_LIBS)

libmymap.so: mymap
	$(CC) $(LIBMYMAP_FLAGS) $(BUILD_DIR_MYMAP)/libmymap.so $(BUILD_DIR_MYMAP)/*.o $(LIBMYMAP_LIBS)
	cp $(BUILD_DIR_MYMAP)/libmymap.so ../../../../target/classes/

clean:
	$(RMR) $(BUILD_DIR_MYMAP)