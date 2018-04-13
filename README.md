# ASystem

A pluggable set of IoT modules.

# Bootstrap

To verify and boostrap the build environment:

```bash
. bootstrap.sh
```

# Install

This project can be compiled, packaged and installed to a local repository, skipping tests, as per:

```bash
mvn install -PPKG
```

To only compile the project:

```bash
mvn install -PCMP
```

To run the unit and system tests:

```bash
mvn test
```

# Release

To perform a release:

```bash
bootstrap.sh release
```
