#!/bin/bash
#
# Bash command completion script for the geowave cli tool
#

__geowave() {
  local curr_arg=${COMP_WORDS[COMP_CWORD]}
  local prev_arg=${COMP_WORDS[COMP_CWORD - 1]}
  case ${COMP_CWORD} in
  1)
    COMPREPLY=( $(compgen -W '-clear -hdfsingest -hdfsstage -kafkastage -localingest -poststage -stats' -- $curr_arg ) )
    ;;
  2)
     case ${prev_arg} in
        -clear)
            COMPREPLY=( $(compgen -W '-datastore -dim --dimensionality -f --formats -h --help -instance -l --list -gwNamespace -password -user -v --visibility -zookeeper' -- $curr_arg ) )
            ;;
        -hdfsingest)
            COMPREPLY=( $(compgen -W '-b --base -datastore -dim --dimensionality -f --formats -h --help -hdfs -hdfsbase -instance -jobtracker -l --list -gwNamespace -password -resourceman -user -v --visibility -x --extension -zookeeper' -- $curr_arg ) )
            ;;
        -hdfsstage)
            COMPREPLY=( $(compgen -W '-b --base -f --formats -h --help -hdfs -hdfsbase -l --list -x --extension' -- $curr_arg ) )
            ;;
        -kafkastage)
            COMPREPLY=( $(compgen -W '-b --base -f --formats -h --help -kafkaprops -kafkatopic -l --list -x --extension' -- $curr_arg ) )
            ;;
        -localingest)
            COMPREPLY=( $(compgen -W '-b --base -datastore -dim --dimensionality -f --formats -h --help -instance -l --list -gwNamespace -p --password -user -v --visibility -x --extension -zookeeper' -- $curr_arg ) )
            ;;
        -poststage)
            COMPREPLY=( $(compgen -W '-datastore -dim --dimensionality -f --formats -h --help -hdfs -hdfsbase -instance -jobtracker -l --list -gwNamespace -password -resourceman -user -v --visibility -zookeeper' -- $curr_arg ) )
            ;;
        -stats)
            COMPREPLY=( $(compgen -W '-datastore -instance -user -password -gwNamespace -v --visibility -zookeeper -type -auth' -- $curr_arg ) )
            ;;
        -zkTx)
            COMPREPLY=( $(compgen -W '-datastore -h --help -instance -m --maximum -gwNamespace -password -r --recipient -user -zookeeper' -- $curr_arg ) )
            ;;
     esac
    ;;
  *)
    COMPREPLY=()
    ;;
  esac
}

complete -F __geowave geowave
