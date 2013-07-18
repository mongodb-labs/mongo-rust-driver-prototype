Contributing to the MongoDB Rust Driver
=======================================

Thank you for your interest in contributing to the MongoDB Rust driver.

We are building this software together and strongly encourage contributions from the community that are within the guidelines set forth below.

# Supported Versions of Rust
Currently, the driver only supports Rust version 0.7 release. We will be upgrading to 0.8 when it is released; in the meantime, please make changes which are valid on this version.

# Bugfixes and New Features
Before starting on new code, take a look at the existing [issues](http://github.com/10gen-interns/mongo-rust-driver-prototype/issues) to see if your new feature or bugfix has already been started. This prevents you from accidentally working on something that has already been addressed.

# Contributing Guidelines
Once you have begun writing code, please keep in mind the following guidelines:
* Avoid breaking changes, or changes to the external API, whenever possible.
* Write documentation comments for any new items, or update existing doc comments if your changes warrant it.
* Write test cases for your code and ensure they pass. Unit tests should go in a ```tests``` module marked with ```#[cfg(test)]``` in the same file as the code being tested. Integration/functional tests should be placed in their own file in the ```src/libmongo/tests``` folder, and should be added to ```src/libmongo/tests/test.rc```. If you are running an integration test, ensure you have a mongod instance running on localhost:27017, and that you run ```make check MONGOTEST=1```.
* The project uses 4-space indenting with no tabs. Also, before submitting your code, run ```make tidy``` to remove any trailing whitespace. This will help keep our code looking uniform.
