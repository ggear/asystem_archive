#!/usr/bin/env bash

###############################################################################
#
# Bootstrap script
#
###############################################################################

MODE=${1:-""}

[[ ! "${MODE}" = "source" ]] && set -x -e

CLOUD_HOST_ID="i-b2e18030"
CLOUD_HOST_IP="52.63.86.162"
AROUTER_HOST_IP="52.63.86.162"
ANODE_HOST_IP="192.168.1.10"

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
  git status ${1}
  git diff $(git rev-parse HEAD)^ $(git rev-parse HEAD) ${1}/src/main/script/python/${2} | tee /dev/tty | patch -p1 ${1}/src/main/template/python/${2}
  rm -rf ${1}/src/main/template/python/${2}\.*
  mvn clean install -PCMP -pl ${1} > /dev/null
  git diff ${1}/src/main/script/python/${2}
  git status ${1}
}

if [ "${MODE}" = "source" ]; then

  echo "" && echo "Source [asystem]"
  CF_DIR=$(mktemp -d -t cloudera-framework.XXXXXX)
  CF_VERSION_JAVA=1.8
  if [ -z ${JAVA_OPTS+x} ]; then
    export JAVA_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"
  fi
  if [ $(java -version 2>&1 | grep ${CF_VERSION_JAVA} | wc -l) -eq 0 ]; then
    echo "Unable to install system dependent Java "${CF_VERSION_JAVA}", please do so manually"
  fi
  java -version || { echo "Java "${CF_VERSION_JAVA}" not found" ; return 10; }
  CF_VERSION_MAVEN=3.5.2
  CF_VERSION_MAVEN_MAJOR=3.
  if [ -z ${MAVEN_OPTS+x} ]; then
    export MAVEN_OPTS="-Xmx2g -Dmaven.artifact.threads=15 -XX:ReservedCodeCacheSize=512m -Duser.home=${CF_DIR}"
  fi
  if [ $(mvn -version 2>&1 | grep ${CF_VERSION_MAVEN_MAJOR} | wc -l) -eq 0 ]; then
    wget http://apache.mirror.amaze.com.au/maven/maven-3/${CF_VERSION_MAVEN}/binaries/apache-maven-${CF_VERSION_MAVEN}-bin.tar.gz -P ${CF_DIR}
    tar xvzf ${CF_DIR}/apache-maven-${CF_VERSION_MAVEN}-bin.tar.gz -C ${CF_DIR}
    test -d ${HOME}/.m2 && cp -rvf ${HOME}/.m2 ${CF_DIR}
    export PATH=${CF_DIR}/apache-maven-${CF_VERSION_MAVEN}/bin:${PATH}
  fi
  mvn -version || { echo "Maven "${CF_VERSION_MAVEN}" not found" ; return 20; }
  CF_VERSION_SCALA=2.11.8
  CF_VERSION_SCALA_MAJOR=2.11
  if [ $(scala -version 2>&1 | grep ${CF_VERSION_SCALA_MAJOR} | wc -l) -eq 0 ]; then
    wget https://downloads.lightbend.com/scala/${CF_VERSION_SCALA}/scala-${CF_VERSION_SCALA}.tgz -P ${CF_DIR}
    tar xvzf ${CF_DIR}/scala-${CF_VERSION_SCALA}.tgz -C ${CF_DIR}
    export PATH=${CF_DIR}/scala-${CF_VERSION_SCALA}/bin:${PATH}
  fi
  scala -version || { echo "Scala "${CF_VERSION_SCALA}" not found" ; return 30; }
  CF_VERSION_PYTHON=2.7
  if [ $(python --version 2>&1 | grep ${CF_VERSION_PYTHON} | wc -l) -eq 0 ]; then
    echo "Unable to install system dependent CPython "${CF_VERSION_PYTHON}", please do so manually"
  fi
  pip install cm-api && python --version || { echo "Python "${CF_VERSION_PYTHON}" not found" ; return 40; }
  export PATH=$(echo ${PWD}/target/assembly/*/bin):$PATH

elif [ "${MODE}" = "prepare" ]; then

  echo "" && echo "Prepare [asystem]"
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

  echo "" && echo "Teardown [asystem]"
  ec2-instance-resize ${CLOUD_HOST_ID} "t2.micro"

elif [ "${MODE}" = "download" ]; then

  echo "" && echo "Download [asystem-amodel]"
  rm -rf asystem-amodel/src/repo/*
  aws s3 sync s3://asystem-amodel asystem-amodel/src/repo
  echo "" && echo "Download [asystem-astore]"
  aws s3 sync s3://asystem-astore asystem-astore/src/repo

elif [ "${MODE}" = "release" ]; then

  echo "" && echo "Release [asystem]"
  mvn clean install -PCMP -pl . && \
  VERSION_RELEASE=$(grep APP_VERSION= target/classes/application.properties | sed 's/APP_VERSION=*//' | sed 's/-SNAPSHOT*//') && \
  VERSION_HEAD_NUMERIC=$(($(echo $VERSION_RELEASE | sed 's/\.//g')+1)) && \
  VERSION_HEAD=${VERSION_HEAD_NUMERIC:0:2}.${VERSION_HEAD_NUMERIC:2:3}.${VERSION_HEAD_NUMERIC:5:4} && \
  mvn clean install && \
  mvn release:prepare -B \
    -DreleaseVersion=${VERSION_RELEASE} \
    -DdevelopmentVersion=${VERSION_HEAD}-SNAPSHOT -PPKG && \
  git add -A && \
  git commit -m "Update generated files for asystem-${VERSION_RELEASE}" && \
  git tag -d asystem-${VERSION_RELEASE} && \
  git push origin :asystem-${VERSION_RELEASE} && \
  mvn release:prepare -B \
    -DreleaseVersion=${VERSION_RELEASE} \
    -DdevelopmentVersion=${VERSION_HEAD}-SNAPSHOT -PPKG -Dresume=false && \
  mvn release:clean && \
  mvn clean install -PPKG &&
  git add -A && \
  git commit -m "Update generated files for asystem-${VERSION_HEAD}" && \
  git push --all && \
  git tag

elif [ "${MODE}" = "deploy" ]; then

  echo "" && echo "Deploy [asystem-arouter]"
  ssh -tt ${AROUTER_HOST_IP} << EOF
    cd dev/asystem
    git checkout master
    git clean -d -x -f
    git pull -a
    git checkout $(git describe \-\-tags | cut -c1-19)
    mvn clean deploy -pl asystem-arouter -PPKG -am -Dmaven.install-flume.skip=false
    exit
EOF
  echo "" && echo "Deploy [asystem-anode]"
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

  echo "" && echo "Diff [asystem-amodel:dataset.py]"
  git-template-diff "asystem-amodel" "dataset.py"
  echo "" && echo "Diff [asystem-amodel:energyforecast.py]"
  git-template-diff "asystem-amodel" "energyforecast.py"
  echo "" && echo "Diff [asystem-amodel:energyforecast_intraday.py]"
  git-template-diff "asystem-amodel" "energyforecast_intraday.py"

elif [ "${MODE}" = "run" ]; then

  echo "" && echo "Run [asystem-anode]"
  mvn clean install antrun:run@python-run -PCMP -pl asystem-anode

else

  echo "Usage: ${0} <source|prepare|teardown|download|release|deploy|diff|run>"

fi