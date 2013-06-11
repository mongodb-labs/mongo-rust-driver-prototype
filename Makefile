RC = rustc
FLAGS = -L .

all: libs bson test

libs:
	$(RC) --lib -o ./bin/libord_hash.dylib ./src/ord_hash.rs

bson:
	$(RC) $(FLAGS) -o ./bin/bson ./src/bson.rs

test:	
	$(RC) $(FLAGS) --test -o ./test/bson_test bson.rs

clean:
	rm -rf ./bin/*.o
	rm ./bin/*.dylib
	rm -rf ./bin/bson
	rm -rf ./bin/bson_test
	rm -rf ./bin/*.dSYM/*
