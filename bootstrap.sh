#!/usr/bin/env bash
###############################################################################
#
# Bootstrap script
#
###############################################################################

[[ -z "${CDSW_PROJECT_URL}" ]] && [[ -f "/etc/profile" ]] && . /etc/profile

CLOUD_HOST_ID="i-b2e18030"
CLOUD_HOST_IP="52.63.86.162"
AROUTER_HOST_IP="52.63.86.162"
ANODE_HOST_IP="192.168.1.10"

PACKAGE_PERFORMED="false"

function mode_execute {

  if [ "${MODE}" = "environment" ]; then

    echo "" && echo "" && echo "" && echo "Environment [asystem]"
    curl -s https://raw.githubusercontent.com/ggear/cloudera-framework/master/bootstrap.sh > target/bootstrap.sh
    chmod 744 target/bootstrap.sh
    . ./target/bootstrap.sh environment

  elif [ "${MODE}" = "prepare" ]; then

    echo "" && echo "" && echo "" && echo "Prepare [asystem]"
    ./bootstrap.sh prepare_local

  elif [ "${MODE}" = "prepare_local" ]; then

    echo "" && echo "" && echo "" && echo "Prepare local [asystem]"
		sudo launchctl load -w /Library/LaunchDaemons/org.jenkins-ci.plist
		sudo launchctl list org.jenkins-ci
		#open http://macmini-sheryl:8070

		sudo launchctl load -w /Library/LaunchDaemons/com.artifactory.plist
		sudo launchctl list com.artifactory
		#open http://macmini-sheryl:8071

  elif [ "${MODE}" = "prepare_bastion" ]; then

    echo "" && echo "" && echo "" && echo "Prepare bastion [asystem]"
    ec2-instance-resize ${CLOUD_HOST_ID} "c4.2xlarge" "true"
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

  elif [ "${MODE}" = "prepare_cluster" ]; then

    echo "" && echo "" && echo "" && echo "Prepare cluster [asystem]"
    build_package
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh

  elif [ "${MODE}" = "teardown" ]; then

    echo "" && echo "" && echo "" && echo "Teardown [asystem]"
    ./bootstrap.sh teardown_bastion
    ./bootstrap.sh teardown_cluster

  elif [ "${MODE}" = "teardown_local" ]; then

    echo "" && echo "" && echo "" && echo "Teardown local [asystem]"
		sudo launchctl unload -w /Library/LaunchDaemons/org.jenkins-ci.plist
		sudo launchctl unload -w /Library/LaunchDaemons/com.artifactory.plist

  elif [ "${MODE}" = "teardown_bastion" ]; then

    echo "" && echo "" && echo "" && echo "Teardown bastion [asystem]"
    ec2-instance-resize ${CLOUD_HOST_ID} "t2.micro" "false"

  elif [ "${MODE}" = "teardown_cluster" ]; then

    echo "" && echo "" && echo "" && echo "Teardown cluster [asystem]"
    [ ! -f ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh ] && build_package
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh "true" "true"

  elif [ "${MODE}" = "download" ]; then

    echo "" && echo "" && echo "" && echo "Download [asystem]"
    git pull -a
    echo "" && echo "" && echo "" && echo "Download [asystem-amodel]"
    aws s3 sync s3://asystem-amodel asystem-amodel/src/repo --delete
    find ./asystem-amodel/src/repo -type d -empty -delete
    du -cksh asystem-amodel/src/repo
    echo "" && echo "" && echo "" && echo "Download [asystem-astore]"
    aws s3 sync s3://asystem-astore asystem-astore/src/repo --delete
    find ./asystem-astore/src/repo -type d -empty -delete
    du -cksh asystem-astore/src/repo

  elif [ "${MODE}" = "download_anode" ]; then

    echo "" && echo "" && echo "" && echo "Download [asystem-anode]"
    ssh janeandgraham.com -p 8092 "sudo service anode restart"
    sleep 5
    rm -rf asystem-anode/src/main/python/anode/test/pickle
    mkdir -p asystem-anode/src/main/python/anode/test/pickle
    scp -r -P 8092 janeandgraham.com:/etc/anode/anode/* asystem-anode/src/main/python/anode/test/pickle
    du -cksh asystem-anode/src/main/python/anode/test/pickle

  elif [ "${MODE}" = "checkout" ]; then

    echo "" && echo "" && echo "" && echo "Checkout [asystem]"
    git checkout master
    git pull -a
    git status

  elif [ "${MODE}" = "checkout_snapshot" ]; then

    echo "" && echo "" && echo "" && echo "Checkout snapshot [asystem]"
    git checkout -- .
    git checkout master
    git pull -a
    git reset HEAD asystem-anode/src/main/python/anode/test/pickle
    git clean -d -x -f asystem-*/src/main asystem-*/src/test
    git checkout -- .
    git status

  elif [ "${MODE}" = "checkout_release" ]; then

    echo "" && echo "" && echo "" && echo "Checkout release [asystem]"
    [[ -n "$(git status --porcelain)" ]] && exit 1
    git checkout master
    git pull -a
    git checkout $(git describe \-\-tags | cut -c1-19)
    git status

  elif [ "${MODE}" = "compile" ]; then

    echo "" && echo "" && echo "" && echo "Build [asystem]"
    git checkout master
    git pull -a
    build_package "CMP"

  elif [ "${MODE}" = "build" ]; then

    echo "" && echo "" && echo "" && echo "Build [asystem]"
    git checkout master
    git pull -a
    build_package "DFT"

  elif [ "${MODE}" = "package" ]; then

    echo "" && echo "" && echo "" && echo "Package [asystem]"
    git checkout master
    git pull -a
    build_package "PKG"

  elif [ "${MODE}" = "test" ]; then

    echo "" && echo "" && echo "" && echo "Test [asystem]"
    git checkout master
    git pull -a
    build_package "ALL"

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
    mvn clean install -PPKG
    git add -A
    git commit -m "Update generated files for asystem-${VERSION_HEAD}-SNAPSHOT"
    git push --all
    git tag

  elif [ "${MODE}" = "release_remote" ]; then

    echo "" && echo "" && echo "" && echo "Release remote [asystem-arouter]"
    ssh -tt ${CLOUD_HOST_IP} << EOF
      cd dev/asystem
      git clean -d -x -f
      ./bootstrap.sh checkout_snapshot release
      exit
EOF

  elif [ "${MODE}" = "deploy" ]; then

    echo "" && echo "" && echo "" && echo "Deploy [asystem-arouter]"
    ./bootstrap.sh deploy_arouter
    ./bootstrap.sh deploy_anode

  elif [ "${MODE}" = "deploy_arouter" ]; then

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

  elif [ "${MODE}" = "deploy_anode" ]; then

    echo "" && echo "" && echo "" && echo "Deploy [asystem-anode]"
    ssh -tt janeandgraham.com -p 8092 << EOF
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
    mvn install -PCMP -pl asystem-amodel
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:dataset.py]"
    git-template-merge "asystem-amodel" "dataset.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_interday.py]"
    git-template-merge "asystem-amodel" "energyforecast_interday.py"
    echo "" && echo "" && echo "" && echo "Diff [asystem-amodel:energyforecast_intraday.py]"
    git-template-merge "asystem-amodel" "energyforecast_intraday.py"
    mvn install -PCMP -pl asystem-amodel
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
    build_package
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/as-amodel-energyforecast.sh

  elif [ "${MODE}" = "run_astore" ]; then

    echo "" && echo "" && echo "" && echo "Run [asystem-astore]"
    build_package
    ./asystem-amodel/target/assembly/asystem-amodel-*/bin/cldr-provision-altus.sh
    ./asystem-astore/target/assembly/asystem-astore-*/bin/as-astore-process.sh

  else

    usage

  fi

}

function usage {
    set +x +e
    echo "" && echo "Usage: ${0} <commands>"
    echo "" && echo "Commands:"
    echo "  help |"
    echo "  environment |"
    echo "  download | download_anode"
    echo "  checkout | checkout_snapshot | checkout_release"
    echo "  compile | build | package | test | release | release_remote | merge"
    echo "  prepare | prepare_local | prepare_bastion | prepare_cluster | teardown | teardown_local | teardown_bastion | teardown_cluster"
    echo "  run | run_anode | run_amodel | run_astore"
    echo "  deploy | deploy_anode | deploy_arouter"
    echo ""
    exit 1
}

function build_package {
  if [ "$PACKAGE_PERFORMED" != "true" ]; then
    PROFILE="PKG"
    [[ ! -z ${1+x} ]] && PROFILE="$1"
    mvn clean install -P"$PROFILE"
    if [ $? -ne 0 ]; then
      mvn install -PCMP -U
      mvn clean install -P"$PROFILE"
    fi
    PACKAGE_PERFORMED="true"
  fi
}

function ec2-instance-resize {
  aws ec2 stop-instances --instance-ids "$1"
  for TICK in {720..1}; do
    if [ $(aws ec2 describe-instance-status --include-all-instances --instance-id "$1" | grep stopped | wc -l) -gt 0 ]; then
      aws ec2 modify-instance-attribute --instance-id "$1" --instance-type "$2"
      break
    fi
    sleep 1
  done
  aws ec2 start-instances --instance-ids "$1"
  for TICK in {720..1}; do
    if [ $(aws ec2 describe-instance-status --include-all-instances --instance-id "$1" | grep running | wc -l) -gt 0 ]; then
      aws ec2 describe-instance-status --include-all-instances --instance-id "$1"
      break
    fi
    sleep 1
  done
  if [ "$3" = "true" ]; then
    for TICK in {720..1}; do
      if ssh ${AROUTER_HOST_IP} ls > /dev/null; then
        break
      fi
      sleep 1
    done
  fi
}

function git-template-merge {
  git diff  --minimal --inter-hunk-context=1 -U1 ${1}/src/main/script/python/${2} | tee /dev/tty | patch -p1 -R ${1}/src/main/template/python/${2}
  rm -rf ${1}/src/main/template/python/*.py.*
}

TIME=$(date +%s)

[ $# -eq 0 ] && usage
for MODE in "$@"; do
  [[ ! "${MODE}" = "environment" ]] && set -x -e
  mode_execute
done

set +x
TIME="$(($(date +%s) - $TIME))"
echo "" && echo "Pipeline execution took ["$(printf '%02d:%02d:%02d\n' $(($TIME/3600)) $(($TIME%3600/60)) $(($TIME%60)))"] time" && echo ""
