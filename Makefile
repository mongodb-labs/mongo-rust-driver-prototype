# Copyright 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

RC = rustc
RDOC = rustdoc
RDOCFLAGS = --output-style doc-per-mod
CC = gcc
AR = ar rcs
FLAGS = -L ./bin -D unused-unsafe $(USERFLAGS)
CFLAGS = -c -g -Wall -Werror
USERFLAGS =
RM = rm
RMDIR = rmdir -p
MKDIR = mkdir -p

SRC = ./src
BSONDIR = ./src/bson
MONGODIR = ./src/libmongo
BIN = ./bin
TEST = ./test
DOCS = ./docs

.PHONY: test

all: bin libs mongodb

bin:
	$(MKDIR) bin
	$(MKDIR) test
	$(MKDIR) docs

libs: $(BSONDIR)/cast.c
	$(CC) $(CFLAGS) -o $(BIN)/typecast.o $(BSONDIR)/cast.c
	$(AR) $(BIN)/libtypecast.a $(BIN)/typecast.o

bson: $(BSONDIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/bson.rs

mongodb: $(SRC)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(SRC)/mongo.rc

mongo: $(MONGODIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(MONGODIR)/mongo.rs

test: $(SRC)/mongo.rc
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(SRC)/mongo.rc

ex: $(MONGODIR)/test.rs
	$(RC) $(FLAGS) -o $(TEST)/mongo_ex $(MONGODIR)/test.rs

check: test
	$(TEST)/bson_test
	$(TEST)/mongo_test

doc: $(BSONDIR)/*.rs $(MONGODIR)/*
	$(MKDIR) docs
	$(RDOC) $(RDOCFLAGS) --output-dir $(DOCS) $(SRC)/mongo.rc

clean:
	$(RM) $(BIN)/*.dylib
	$(RM) -rf $(TEST)
	$(RM) -rf $(BIN)
	$(RM) -rf $(DOCS)

tidy:
	sed -e 's/\s\+$$//g' ./src/*
