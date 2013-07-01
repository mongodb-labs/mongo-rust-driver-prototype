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
RDOCFLAGS = --output-dir $(DOCS) --output-format markdown --output-style doc-per-mod
CC = gcc
AR = ar rcs
FLAGS = -L ./bin $(USERFLAGS)
CFLAGS = -c -g -Wall -Werror 
USERFLAGS = 
RM = rm
RMDIR = rmdir -p
MKDIR = mkdir -p

BSONDIR = ./src/bson
MONGODIR = ./src/libmongo
#MOXDIR = ./src/moxidize
BIN = ./bin
TEST = ./test
DOCS = ./docs

.PHONY: test

all: bin libs bson mongo
#all: bin libs bson moxidize mongo

bin:
	$(MKDIR) bin
	$(MKDIR) test
	$(MKDIR) docs

libs: $(BSONDIR)/cast.c
	$(CC) $(CFLAGS) -o $(BIN)/typecast.o $(BSONDIR)/cast.c
	$(AR) $(BIN)/libtypecast.a $(BIN)/typecast.o

bson: $(BSONDIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/bson.rc

#moxidize: $(MOXDIR)/*
#	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(MOXDIR)/moxidize.rc

mongo: $(MONGODIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(MONGODIR)/mongo.rc

test: $(BSONDIR)/bson.rc $(MONGODIR)/mongo.rc
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(BSONDIR)/bson.rc
#	$(RC) $(FLAGS) --test -o $(TEST)/mox_test $(MOXDIR)/moxidize.rc
	$(RC) $(FLAGS) --test -o $(TEST)/mongo_test $(MONGODIR)/mongo.rc

check: test 
	$(TEST)/bson_test
	$(TEST)/mongo_test

doc: $(BSONDIR)/*.rs $(MONGODIR)/*
	$(RDOC) $(RDOCFLAGS) $(BSONDIR)/bson.rc
	$(RDOC) $(RDOCFLAGS) $(MONGODIR)/mongo.rc

clean:
	$(RM) $(BIN)/*.dylib
	$(RM) -rf $(TEST)
	$(RM) -rf $(BIN)
	$(RM) -rf $(DOCS)

tidy:
	sed -e 's/\s\+$$//g' ./src/*
