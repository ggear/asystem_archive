# ASystem

A pluggable set of home IoT modules.

## Bootstrap

To bootstrap and verify the build environment:

```bash
./bootstrap.sh environment
```

## Build

To compile, package, test and install the project to a local repository:

```bash
./bootstrap.sh build
```

## Release

To perform a release to the code and artifact repositories:

```bash
./bootstrap.sh release
```

## Deploy

To deploy the latest release to production:

```bash
./bootstrap.sh deploy
```

## Pipeline

To perform a full release, deploy, run pipeline:

```bash
./bootstrap.sh prepare release deploy run teardown
```

or alternatively, to run the most recent release: 

```bash
./bootstrap.sh checkout_release prepare run teardown checkout
```

