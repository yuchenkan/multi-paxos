set -ex
set -o pipefail

log=`date +%Y_%m_%d_%H_%M_%S`
conf=`cat debug.conf`

rm -rf $log
mkdir $log
./main $conf $log false 2>&1 | tee run_$log.log

if [ "$1" == true ]
then
    ./main $conf $log true 2>&1 | tee run_"$log"_replay.log

    diff run_$log.log run_"$log"_replay.log
fi

echo done
