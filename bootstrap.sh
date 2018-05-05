#!/usr/bin/env bash

###############################################################################
#
# Bootstrap script
#
###############################################################################

CLOUD_HOST_ID="i-b2e18030"
CLOUD_HOST_IP="52.63.86.162"
AROUTER_HOST_IP="52.63.86.162"
ANODE_HOST_IP="192.168.1.10"

function mode_execute {

  if [ "${MODE}" = "env" ]; then

    echo "" && echo "" && echo "" && echo "Source [asystem]"
    curl -s https://raw.githubusercontent.com/ggear/cloudera-framework/master/bootstrap.sh > target/bootstrap.sh
    chmod 744 target/bootstrap.sh
    . ./target/bootstrap.sh

  elif [ "${MODE}" = "prepare" ]; then

    echo "" && echo "" && echo "" && echo "Prepare [asystem]"
    ec2-instance-resize ${CLOUD_HOST_ID} "m4.xlarge"
    ssh -tt ${CLOUD_HOST_IP} << EOF
      sudo service cloudera-scm-server-db start
      sudo service cloudera-scm-agent start
      sudo service cloudera-scm-server start
      sudo service jenkins start
      exit
EOF
    for TICK in {720..1}; do
      if curl -m 5 http://${AROUTER_HOST_IP}:7180/cmf/login > /dev/null 2>&1; then
        break
      fi
      sleep 1
    done

  elif [ "${MODE}" = "teardown" ]; then

    echo "" && echo "" && echo "" && echo "Teardown [asystem]"
    ec2-instance-resize ${CLOUD_HOST_ID} "t2.micro"
    mvn clean install -PPKG
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh "false" "true"

  elif [ "${MODE}" = "download" ]; then

    echo "" && echo "" && echo "" && echo "Download [asystem-amodel]"
    rm -rf asystem-amodel/src/repo/*
    aws s3 sync s3://asystem-amodel asystem-amodel/src/repo
    echo "" && echo "" && echo "" && echo "Download [asystem-astore]"
    aws s3 sync s3://asystem-astore asystem-astore/src/repo --delete
    du -cksh asystem-astore/src/repo

  elif [ "${MODE}" = "build" ]; then

    echo "" && echo "" && echo "" && echo "Build [asystem]"
    mvn clean install

  elif [ "${MODE}" = "release" ]; then

    echo "" && echo "" && echo "" && echo "Release [asystem]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout master
    mvn clean install -PCMP -pl .
    VERSION_RELEASE=$(grep APP_VERSION= target/classes/application.properties | sed 's/APP_VERSION=*//' | sed 's/-SNAPSHOT*//')
    VERSION_HEAD_NUMERIC=$(($(echo $VERSION_RELEASE | sed 's/\.//g')+1))
    VERSION_HEAD=${VERSION_HEAD_NUMERIC:0:2}.${VERSION_HEAD_NUMERIC:2:3}.${VERSION_HEAD_NUMERIC:5:4}
    mvn clean install
    mvn release:prepare -B \
      -DreleaseVersion=${VERSION_RELEASE} \
      -DdevelopmentVersion=${VERSION_HEAD}-SNAPSHOT -PPKG
    git add -A
    git commit -m "Update generated files for asystem-${VERSION_RELEASE}"
    git tag -d asystem-${VERSION_RELEASE}
    git push origin :asystem-${VERSION_RELEASE}
    mvn release:prepare -B \
      -DreleaseVersion=${VERSION_RELEASE} \
      -DdevelopmentVersion=${VERSION_HEAD}-SNAPSHOT -PPKG -Dresume=false
    mvn release:clean
    mvn clean install -PPKG &&
    git add -A
    git commit -m "Update generated files for asystem-${VERSION_HEAD}"
    git push --all
    git tag

  elif [ "${MODE}" = "deploy" ]; then

    echo "" && echo "" && echo "" && echo "Deploy [asystem-arouter]"
    ssh -tt ${AROUTER_HOST_IP} << EOF
      cd dev/asystem
      git checkout master
      git clean -d -x -f
      git pull -a
      git checkout $(git describe \-\-tags | cut -c1-19)
      mvn clean deploy -pl asystem-arouter -PPKG -am -Dmaven.install-flume.skip=false
      exit
EOF
    echo "" && echo "" && echo "" && echo "Deploy [asystem-anode]"
    ssh -tt ${ANODE_HOST_IP} << EOF
      cd dev/asystem
      git checkout master
      git clean -d -x -f
      git pull -a
      git checkout $(git describe \-\-tags | cut -c1-19)
      mvn clean deploy -pl asystem-anode -PPKG -Dmaven.install-python.skip=false
      exit
EOF

  elif [ "${MODE}" = "diff" ]; then

    [[ -n "$(git status --porcelain asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python)" ]] && exit 1
    git checkout master
    git pull --all
    mvn clean install -PCMP -pl asystem-amodel
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:dataset.py]"
    git-template-diff "asystem-amodel" "dataset.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_interday.py]"
    git-template-diff "asystem-amodel" "energyforecast_interday.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_intraday.py]"
    git-template-diff "asystem-amodel" "energyforecast_intraday.py"
    mvn clean install -PCMP -pl asystem-amodel
    git diff asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python
    git status asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python

  elif [ "${MODE}" = "runlocal" ]; then

    echo "" && echo "" && echo "" && echo "Run local [asystem-anode]"
    mvn clean install antrun:run@python-run -PCMP -pl asystem-anode

  elif [ "${MODE}" = "runsnap" ]; then

    echo "" && echo "" && echo "" && echo "Run snapshot [asystem]"
    git checkout master
    mvn clean install -PPKG
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-astore/target/assembly/asystem-astore-*/bin/as-astore-process.sh
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/as-amodel-energyforecast.sh

  elif [ "${MODE}" = "runtag" ]; then

    echo "" && echo "" && echo "" && echo "Run tag [asystem]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout $(git describe \-\-tags | cut -c1-19)
    mvn clean install -PPKG
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-astore/target/assembly/asystem-astore-*/bin/as-astore-process.sh
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/as-amodel-energyforecast.sh
    git checkout master

  else

    echo "Usage: ${0} <env|prepare|download|build|release|deploy|diff|runlocal|runsnap|runtag|teardown>"

  fi

}

function ec2-instance-resize {
  ec2-stop-instances "$1"
  for TICK in {720..1}; do
    if [ $(ec2-describe-instance-status -A "$1" | grep stopped | wc -l) -gt 0 ]; then
      ec2-modify-instance-attribute "$1" --instance-type "$2"
      break
    fi
    sleep 1
  done
  ec2-start-instances "$1"
  for TICK in {720..1}; do
    if [ $(ec2-describe-instance-status -A "$1" | grep running | wc -l) -gt 0 ]; then
      ec2-describe-instance-status "$1"
      break
    fi
    sleep 1
  done
  for TICK in {720..1}; do
    if ssh ${AROUTER_HOST_IP} ls > /dev/null; then
      break
    fi
    sleep 1
  done
}

function git-template-diff {
  git diff ${1}/src/main/script/python/${2} | tee /dev/tty | patch -p1 -R ${1}/src/main/template/python/${2}
  rm -rf ${1}/src/main/template/python/*.py.*
}

for MODE in "$@"; do
  [[ ! "${MODE}" = "source" ]] && set -x -e
  mode_execute
done

