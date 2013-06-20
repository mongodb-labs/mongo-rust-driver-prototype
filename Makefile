RC = rustc
RDOC = rustdoc --output-dir $(DOCS) --output-format markdown --output-style doc-per-mod
CC = gcc
AR = ar rcs
FLAGS = -L ./bin
CFLAGS = -c -g -o
RM = rm
RMDIR = rmdir -p
MKDIR = mkdir -p

BSONDIR = ./src/bson
BIN = ./bin
TEST = ./test
DOCS = ./docs

.PHONY: test

all: bin libs test bson

bin:
	$(MKDIR) bin
	$(MKDIR) test
	$(MKDIR) docs

libs: $(BSONDIR)/ord_hash.rs $(BSONDIR)/stream.rs $(BSONDIR)/json_parse.rs $(BSONDIR)/bson_types.rs $(BSONDIR)/cast.c
	$(CC) $(CFLAGS) $(BIN)/typecast.o $(BSONDIR)/cast.c
	$(AR) $(BIN)/libtypecast.a $(BIN)/typecast.o
	$(RC) --lib --out-dir $(BIN) $(BSONDIR)/ord_hash.rs
	$(RC) --lib --out-dir $(BIN) $(BSONDIR)/stream.rs
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/bson_types.rs
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/json_parse.rs

bson: $(BSONDIR)/bson.rs
	$(RC) $(FLAGS) -o $(BIN)/bson $(BSONDIR)/bson.rs

test: $(BSONDIR)/bson.rs $(BSONDIR)/stream.rs $(BSONDIR)/json_parse.rs
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(BSONDIR)/bson.rs
	$(RC) $(FLAGS) --test -o $(TEST)/stream_test $(BSONDIR)/stream.rs
	$(RC) $(FLAGS) --test -o $(TEST)/json_test $(BSONDIR)/json_parse.rs

runtests: $(TEST)/bson_test $(TEST)/stream_test $(TEST)/json_test
	$(TEST)/bson_test
	$(TEST)/stream_test
	$(TEST)/json_test

doc: $(BSONDIR)/ord_hash.rs $(BSONDIR)/stream.rs $(BSONDIR)/json_parse.rs $(BSONDIR)/bson_types.rs $(BSONDIR)/bson.rs
	$(RDOC) $(BSONDIR)/ord_hash.rs
	$(RDOC) $(BSONDIR)/stream.rs
	$(RDOC) $(BSONDIR)/json_parse.rs
	$(RDOC) $(BSONDIR)/bson_types.rs
	$(RDOC) $(BSONDIR)/bson.rs

clean:
	$(RM) $(BIN)/*.dylib
	$(RM) -rf $(TEST)
	$(RM) -rf $(BIN)
	$(RM) -rf $(DOCS)
