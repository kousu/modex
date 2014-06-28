#util.sh
# a series of utility functions
# (which should be in core bash, but we do what we must)

# abspath:
# coerce paths to absolute paths
function abspath() { #TODO: factor into util.sh
 (cd "$1" && pwd); # the 'cd' stops taking effect after this function ends, because of the subshell
 # BUG: error checking? how can abspath signal an error? by bash is weak. I want any abspath failures to be script failures
 # --> right now, given a null $1 the result is that this returns pwd
 # this function guarantees *NO* trailing slash
 # and removes all unnecessary "./" and "../"
}

# TODO: figure out how to factor out here(), which does "$(abspath $(dirname $0))" where $0 is the name of the script
# $0 gets set to something else inside of any function
# so this isn't going to work as written