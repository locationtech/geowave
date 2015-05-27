#!/bin/bash
#
# Bash command completion script for the geowave cli tool
#

__geowave() {
  local curr_arg=${COMP_WORDS[COMP_CWORD]}
  local prev_arg=${COMP_WORDS[COMP_CWORD - 1]}
  case ${COMP_CWORD} in
  1)
    COMPREPLY=( $(compgen -W '-clear -hdfsingest -hdfsstage -kafkastage -localingest -poststage -stats -zkTx' -- $curr_arg ) )
    ;;
  2)
     case ${prev_arg} in
        -clear)
            COMPREPLY=( $(compgen -W '-c --clear -dim --dimensionality -f --formats -h --help -i --instance-id -l --list -n --namespace -p --password -u --user -v --visibility -z --zookeepers' -- $curr_arg ) )
            ;;
        -hdfsingest)
            COMPREPLY=( $(compgen -W '-b --base -c --clear -dim --dimensionality -f --formats -h --help -hdfs -hdfsbase -i --instance-id -jobtracker -l --list -n --namespace -p --password -resourceman -u --user -v --visibility -x --extension -z --zookeepers' -- $curr_arg ) )
            ;;
        -hdfsstage)
            COMPREPLY=( $(compgen -W '-b --base -f --formats -h --help -hdfs -hdfsbase -l --list -x --extension' -- $curr_arg ) )
            ;;
        -kafkastage)
            COMPREPLY=( $(compgen -W '-b --base -f --formats -h --help -kafkaprops -kafkatopic -l --list -x --extension' -- $curr_arg ) )
            ;;
        -localingest)
            COMPREPLY=( $(compgen -W '-b --base -c --clear -dim --dimensionality -f --formats -h --help -i --instance-id -l --list -n --namespace -p --password -u --user -v --visibility -x --extension -z --zookeepers' -- $curr_arg ) )
            ;;
        -poststage)
            COMPREPLY=( $(compgen -W '-c --clear -dim --dimensionality -f --formats -h --help -hdfs -hdfsbase -i --instance-id -jobtracker -l --list -n --namespace -p --password -resourceman -u --user -v --visibility -z --zookeepers' -- $curr_arg ) )
            ;;
        -stats)
            COMPREPLY=( $(compgen -W '-i --instance-id -u --user -p --password -n --namespace -v --visibility -z --zookeepers -type -auth' -- $curr_arg ) )
            ;;
        -zkTx)
            COMPREPLY=( $(compgen -W '-h --help -i --instance-id -m --maximum -n --namespace -p --password -r --recipient -u --user -z --zookeepers' -- $curr_arg ) )
            ;;
     esac
    ;;
  *)
    COMPREPLY=()
    ;;
  esac
}

complete -F __geowave geowave
