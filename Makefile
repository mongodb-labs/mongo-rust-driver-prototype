RC = rustc
CC = gcc
AR = ar rcs
FLAGS = -L ./bin
CFLAGS = -c -g -o
RM = rm
RMDIR = rmdir -p
MKDIR = mkdir -p

SRC = ./src
BIN = ./bin
TEST = ./test

.PHONY: test

all: bin libs test bson

bin:
	$(MKDIR) bin
	$(MKDIR) test

libs: $(SRC)/ord_hash.rs $(SRC)/stream.rs $(SRC)/json_parse.rs $(SRC)/bson_types.rs $(SRC)/cast.c
	$(CC) $(CFLAGS) $(BIN)/typecast.o $(SRC)/cast.c
	$(AR) $(BIN)/libtypecast.a $(BIN)/typecast.o
	$(RC) --lib --out-dir $(BIN) $(SRC)/ord_hash.rs
	$(RC) --lib --out-dir $(BIN) $(SRC)/stream.rs
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(SRC)/bson_types.rs
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(SRC)/json_parse.rs

bson: $(SRC)/bson.rs
	$(RC) $(FLAGS) -o $(BIN)/bson $(SRC)/bson.rs

test: $(SRC)/bson.rs $(SRC)/stream.rs $(SRC)/json_parse.rs
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(SRC)/bson.rs
	$(RC) $(FLAGS) --test -o $(TEST)/stream_test $(SRC)/stream.rs
	$(RC) $(FLAGS) --test -o $(TEST)/json_test $(SRC)/json_parse.rs

runtests: $(TEST)/bson_test $(TEST)/stream_test $(TEST)/json_test
	$(TEST)/bson_test
	$(TEST)/stream_test
	$(TEST)/json_test

clean:
	$(RM) $(BIN)/*.dylib
	$(RM) -f $(BIN)/bson
	$(RM) -rf $(TEST)/*
	$(RM) -f 
	$(RM) -rf $(BIN)/*
	$(RMDIR) bin
	$(RMDIR) test
