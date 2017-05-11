#!/bin/bash


status(){
        ps -ef|egrep "objectStoreMonitor.py"|grep -v grep
        return $?
}

currentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $currentDir

status > /dev/null 2>&1
if [ $? -ne 0 ]
then
    export PYTHONPATH=$PYTHONPATH:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/rucio-clients/current/lib/python2.7/site-packages/
    python $currentDir/objectStoreMonitor.py
fi

