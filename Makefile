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
MONGODIR = ./src/libmongo
BIN = ./bin
TEST = ./test
DOCS = ./docs

.PHONY: test

all: bin libs bson mongo test

bin:
	$(MKDIR) bin
	$(MKDIR) test
	$(MKDIR) docs

libs: $(BSONDIR)/cast.c
	$(CC) $(CFLAGS) $(BIN)/typecast.o $(BSONDIR)/cast.c
	$(AR) $(BIN)/libtypecast.a $(BIN)/typecast.o

bson: $(BSONDIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/bson.rc

mongo: $(MONGODIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(MONGODIR)/mongo.rc

test: $(BSONDIR)/bson.rc $(MONGODIR)/mongo.rc
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(BSONDIR)/bson.rc
	$(RC) $(FLAGS) --test -o $(TEST)/mongo_test $(MONGODIR)/mongo.rc

runtests: $(TEST)/*
	$(TEST)/bson_test
	$(TEST)/mongo_test

doc: $(BSONDIR)/ord_hash.rs $(BSONDIR)/stream.rs $(BSONDIR)/json_parse.rs $(BSONDIR)/bson_types.rs $(BSONDIR)/bson.rs $(MONGODIR)/*
	$(RDOC) $(BSONDIR)/ord_hash.rs
	$(RDOC) $(BSONDIR)/stream.rs
	$(RDOC) $(BSONDIR)/json_parse.rs
	$(RDOC) $(BSONDIR)/bson_types.rs
	$(RDOC) $(BSONDIR)/bson.rs
	$(RDOC) $(MONGODIR)/cursor.rs

clean:
	$(RM) $(BIN)/*.dylib
	$(RM) -rf $(TEST)
	$(RM) -rf $(BIN)
	$(RM) -rf $(DOCS)
