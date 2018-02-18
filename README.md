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
export AS_VERSION_RELEASE=10.000.0017
export AS_VERSION_HEAD=10.000.0018
mvn clean install && \
mvn release:prepare -B \
  -DreleaseVersion=$AS_VERSION_RELEASE \
  -DdevelopmentVersion=$AS_VERSION_HEAD-SNAPSHOT -PPKG && \
git add -A && \
git commit -m "Update generated files for asystem-${AS_VERSION_RELEASE}" && \
git tag -d asystem-$AS_VERSION_RELEASE && \
git push origin :asystem-$AS_VERSION_RELEASE && \
mvn release:prepare -B \
  -DreleaseVersion=$AS_VERSION_RELEASE \
  -DdevelopmentVersion=$AS_VERSION_HEAD-SNAPSHOT -PPKG -Dresume=false && \
mvn release:clean && \
mvn clean install -PPKG &&
git add -A && \
git commit -m "Update generated files for asystem-${AS_VERSION_HEAD}" && \
git push --all && \
git tag
```
