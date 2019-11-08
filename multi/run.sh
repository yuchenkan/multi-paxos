set -ex

set -o pipefail

./main `cat debug.conf` 2>&1 | tee run_`date +%Y_%m_%d_%H_%M_%S`.log
