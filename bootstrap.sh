#!/usr/bin/env bash
###############################################################################
#
# Bootstrap script
#
###############################################################################

[[ -f "/etc/profile" ]] && . /etc/profile

CLOUD_HOST_ID="i-b2e18030"
CLOUD_HOST_IP="52.63.86.162"
AROUTER_HOST_IP="52.63.86.162"
ANODE_HOST_IP="192.168.1.10"

function mode_execute {

  if [ "${MODE}" = "environment" ]; then

    echo "" && echo "" && echo "" && echo "Environment [asystem]"
    curl -s https://raw.githubusercontent.com/ggear/cloudera-framework/master/bootstrap.sh > target/bootstrap.sh
    chmod 744 target/bootstrap.sh
    . ./target/bootstrap.sh environment

  elif [ "${MODE}" = "prepare" ]; then

    echo "" && echo "" && echo "" && echo "Prepare [asystem]"
    ec2-instance-resize ${CLOUD_HOST_ID} "c4.4xlarge"
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
    ./bootstrap.sh teardown_cluster

  elif [ "${MODE}" = "teardown_cluster" ]; then

    echo "" && echo "" && echo "" && echo "Teardown cluster [asystem]"
    mvn clean -pl asystem-astore
    mvn install -PPKG
    ./asystem-astore/target/assembly/asystem-astore-*/bin/cldr-provision-altus.sh "false" "true"

  elif [ "${MODE}" = "download" ]; then

    echo "" && echo "" && echo "" && echo "Download [asystem]"
    git pull -a
    echo "" && echo "" && echo "" && echo "Download [asystem-amodel]"
    aws s3 sync s3://asystem-amodel asystem-amodel/src/repo --delete
    du -cksh asystem-amodel/src/repo
    echo "" && echo "" && echo "" && echo "Download [asystem-astore]"
    aws s3 sync s3://asystem-astore asystem-astore/src/repo --delete
    du -cksh asystem-astore/src/repo

  elif [ "${MODE}" = "download_anode" ]; then

    echo "" && echo "" && echo "" && echo "Download [asystem-anode]"
    rm -rf asystem-anode/src/main/python/anode/test/pickle
    mkdir -p asystem-anode/src/main/python/anode/test/pickle
    scp -r -P 8092 janeandgraham.com:/etc/anode/anode/* asystem-anode/src/main/python/anode/test/pickle
    du -cksh asystem-anode/src/main/python/anode/test/pickle

  elif [ "${MODE}" = "checkout" ]; then

    echo "" && echo "" && echo "" && echo "Checkout [asystem]"
    git checkout master
    rm -rf asystem-anode/src/main/python/anode/test/pickle
    git checkout -- asystem-anode/src/main/python/anode/test/pickle
    git status
    mvn clean

  elif [ "${MODE}" = "checkout_release" ]; then

    echo "" && echo "" && echo "" && echo "Checkout release [asystem]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout $(git describe \-\-tags | cut -c1-19)
    git status

  elif [ "${MODE}" = "build" ]; then

    echo "" && echo "" && echo "" && echo "Build [asystem]"
    git checkout master
    git pull -a
    mvn clean install

  elif [ "${MODE}" = "release" ]; then

    echo "" && echo "" && echo "" && echo "Release [asystem]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout master
    git remote set-url origin git@github.com:ggear/asystem.git
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
    git commit -m "Update generated files for asystem-${VERSION_HEAD}-SNAPSHOT"
    git push --all
    git tag

  elif [ "${MODE}" = "deploy" ]; then

    echo "" && echo "" && echo "" && echo "Deploy [asystem-arouter]"
    ssh -tt ${AROUTER_HOST_IP} << EOF
      cd dev/asystem
      git checkout master
      git clean -d -x -f
      git checkout -- .
      git pull -a
      git checkout $(git describe \-\-tags | cut -c1-19)
      mvn clean deploy -pl asystem-arouter -PPKG -am -Dmaven.install-flume.skip=false -U
      exit
EOF
    echo "" && echo "" && echo "" && echo "Deploy [asystem-anode]"
    ssh -tt ${ANODE_HOST_IP} << EOF
      cd dev/asystem
      git checkout master
      sudo git clean -d -x -f
      git checkout -- .
      git pull -a
      git checkout $(git describe \-\-tags | cut -c1-19)
      mvn clean deploy -pl asystem-anode -PPKG -Dmaven.install-python.skip=false -U
      exit
EOF

  elif [ "${MODE}" = "merge" ]; then

    [[ -n "$(git status --porcelain asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python)" ]] && exit 1
    git checkout master
    git pull -a
    mvn clean install -PCMP -pl asystem-amodel
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:dataset.py]"
    git-template-merge "asystem-amodel" "dataset.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_interday.py]"
    git-template-merge "asystem-amodel" "energyforecast_interday.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_intraday.py]"
    git-template-merge "asystem-amodel" "energyforecast_intraday.py"
    mvn clean install -PCMP -pl asystem-amodel
    git diff asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python
    git status asystem-amodel/src/main/script asystem-amodel/src/main/python asystem-amodel/src/main/template/python

  elif [ "${MODE}" = "run" ]; then

    echo "" && echo "" && echo "" && echo "Run [asystem]"
    ./bootstrap.sh run_astore
    ./bootstrap.sh run_amodel

  elif [ "${MODE}" = "run_anode" ]; then

    echo "" && echo "" && echo "" && echo "Run [asystem-anode]"
    mvn clean install antrun:run@python-run -Dconda.build.skip=false -PCMP -pl asystem-anode

  elif [ "${MODE}" = "run_amodel" ]; then

    echo "" && echo "" && echo "" && echo "Run [asystem-amodel]"
    mvn clean install -PPKG
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/as-amodel-energyforecast.sh

  elif [ "${MODE}" = "run_astore" ]; then

    echo "" && echo "" && echo "" && echo "Run [asystem-astore]"
    mvn clean install -PPKG
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-astore/target/assembly/asystem-astore-*/bin/as-astore-process.sh

  else

    echo "Usage: ${0} <environment|prepare|download|download_anode|checkout|checkout_release|build|release|deploy|merge|run|run_anode|run_amodel|run_astore|teardown|teardown_cluster>"

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

function git-template-merge {
  git diff  --minimal --inter-hunk-context=1 -U1 ${1}/src/main/script/python/${2} | tee /dev/tty | patch -p1 -R ${1}/src/main/template/python/${2}
  rm -rf ${1}/src/main/template/python/*.py.*
}

for MODE in "$@"; do
  [[ ! "${MODE}" = "environment" ]] && set -x -e
  mode_execute
done

