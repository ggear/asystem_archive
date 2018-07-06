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
./bootstrap.sh checkout_snapshot prepare release_remote checkout_release run teardown_cluster checkout_snapshot deploy teardown download
```

## Videos

There are a number of videos that step through the project from the perspective of outlining a 
[continuously delivered pipeline](https://www.youtube.com/watch?v=Z-OFCi2AQfw):

* [Demo, Part 1](https://www.youtube.com/watch?v=9t5ocDLdlS0)
* [Demo, Part 2](https://www.youtube.com/watch?v=Xc6F4I6AyRo)
* [Demo, Part 3](https://www.youtube.com/watch?v=aqIy9qnjqa8)
* [Demo, Part 4](https://www.youtube.com/watch?v=I-l5LKll5GM)
* [Demo, Part 5](https://www.youtube.com/watch?v=yx1E_e6xb5E)
