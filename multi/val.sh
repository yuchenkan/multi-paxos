set -ex

set -o pipefail

valgrind ./main `cat debug.conf` 2>&1 | tee val_`date +%Y_%m_%d_%H_%M_%S`.log
