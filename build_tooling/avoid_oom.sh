#!/bin/bash

# Avoids compiling $file with other parallel compiler executions

file=expression_node.cpp
pid_file=/var/run/$file.pid

function is_running() { ps -p $pid >/dev/null ; }
function cc_pids() { ps -eo pid,cmd | grep $@ | awk "/${cmd:-cc1plus}/ && ! /sccache|grep|bash/ {ORS=\" \" ; print ${pattern:-\$1} }" ; }

if [[ "$*" == *${file}* ]] ; then
    pid=$$
    echo $pid > $pid_file

    (
        sleep 10
        if is_running $pid ; then
            others=$(pattern='"--pid " $1' cc_pids -v $file)

            if [[ -n "$others" ]] ; then
                echo "Going to pause $file compilation to let others ($others) finish first" >&2
                file_cc1=`cc_pids $file`

                kill -STOP $file_cc1
                eval "tail -f /dev/null $others"

                echo "Resuming compilation of $file" >&2
                kill -CONT $file_cc1
            fi
            tail --pid=$pid -f /dev/null
        fi
        rm $pid_file
    ) &

else
    pid=`cat $pid_file 2>/devnull`
    if [[ -n "$pid" ]] && is_running $pid ; then
        echo "Waiting for $file compilation to finish before starting new ones (pid $$)" >&2
        tail --pid=$pid -f /dev/null
        echo "Starting compilation on pid $$" >&2
    fi
fi

exec sccache "$@"
