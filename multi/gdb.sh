set -ex

gdb --args ./main `cat debug.conf`
