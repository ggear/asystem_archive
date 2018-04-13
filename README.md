# ASystem

A pluggable set of IoT modules.

# Bootstrap

To verify and bootstrap the build environment:

```bash
. bootstrap.sh
```

# Install

This project uses maven to mange its build.

To compile, package and install the project to a local repository, skipping tests:

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

To bootstrap and perform a release:

```bash
bootstrap.sh release
```
