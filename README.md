# ASystem

A pluggable set of IoT modules.

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
# Change the following variables to appropriate values for the target release
export AS_VERSION_RELEASE=1.5.8
export AS_VERSION_HEAD=1.5.9
mvn clean install && \
mvn release:prepare -B \
  -DreleaseVersion=$AS_VERSION_RELEASE \
  -DdevelopmentVersion=$AS_VERSION_HEAD-SNAPSHOT -PPKG && \
mvn release:perform -PPKG && \
git push --all && \
git tag
```
