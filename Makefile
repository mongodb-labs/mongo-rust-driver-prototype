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
RDOCFLAGS = --output-style doc-per-mod --output-format markdown
CC = gcc
AR = ar rcs
FLAGS = -Z debug-info -L ./bin -D unused-unsafe -A unnecessary-allocation $(TOOLFLAGS)
CFLAGS = -c -g -Wall -Werror
RM = rm
RMDIR = rmdir -p
MKDIR = mkdir -p

SRC = ./src
LIB = ./lib
BSONDIR = ./src/libbson
MONGODIR = ./src/libmongo
EXDIR = ./examples
BIN = ./bin
TEST = ./test
DOCS = ./docs

MONGOTEST = 0

.PHONY: test

all: bin libs bson mongo

bin:
	$(MKDIR) bin
	$(MKDIR) test

libs: $(LIB)/md5.c
	$(CC) $(CFLAGS) -o $(BIN)/md5.o $(LIB)/md5.c
	$(AR) $(BIN)/libmd5.a $(BIN)/md5.o

bson: $(BSONDIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(BSONDIR)/bson.rc

mongo: $(MONGODIR)/*
	$(RC) $(FLAGS) --lib --out-dir $(BIN) $(MONGODIR)/mongo.rc

test: $(BSONDIR)/bson.rc $(MONGODIR)/mongo.rc
	$(RC) $(FLAGS) --test -o $(TEST)/bson_test $(BSONDIR)/bson.rc
	$(RC) $(FLAGS) --test -o $(TEST)/mongo_test $(MONGODIR)/mongo.rc
	$(RC) $(FLAGS) --test -o $(TEST)/driver_test $(MONGODIR)/test/test.rc

check: test
ifeq ($(MONGOTEST),1)
	$(TEST)/bson_test
	$(TEST)/mongo_test
	$(TEST)/driver_test
else
	$(TEST)/bson_test
	$(TEST)/mongo_test
endif

ex: $(EXDIR)/*
	$(RC) $(FLAGS) $(EXDIR)/bson_demo.rs
	$(RC) $(FLAGS) $(EXDIR)/mongo_demo.rs
	$(RC) $(FLAGS) $(EXDIR)/tutorial.rs

doc: $(BSONDIR)/*.rs $(MONGODIR)/*
	$(MKDIR) docs
	$(MKDIR) docs/bson
	$(MKDIR) docs/mongo
	$(RDOC) $(RDOCFLAGS) --output-dir $(DOCS)/bson $(BSONDIR)/bson.rc
	$(RDOC) $(RDOCFLAGS) --output-dir $(DOCS)/mongo $(MONGODIR)/mongo.rc

clean:
	$(RM) -rf $(TEST)
	$(RM) -rf $(BIN)

tidy:
	for f in `find . -name '*.rs'`; do perl -pi -e "s/[ \t]*$$//" $$f; done
	for f in `find . -name '*.rc'`; do perl -pi -e "s/[ \t]*$$//" $$f; done
