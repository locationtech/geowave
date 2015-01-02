#!/bin/bash
#
# Bash command completion script for GeoWave ingest tool
#

__geowave_ingest() {
  local curr_arg=${COMP_WORDS[COMP_CWORD]}
  local prev_arg=${COMP_WORDS[COMP_CWORD - 1]}
  case ${COMP_CWORD} in
  1)
    COMPREPLY=( $(compgen -W '-clear -hdfsingest -hdfsstage -localingest -poststage' -- $curr_arg ) )
    ;;
  2)
     case ${prev_arg} in
        -clear)
            COMPREPLY=( $(compgen -W '-c --clear -dim --dimensionality -h --help -i --instance-id -l --list -n --namespace -p --password -t --types -u --user -v --visibility -z --zookeepers' -- $curr_arg ) )
            ;;
        -hdfsingest)
            COMPREPLY=( $(compgen -W '-b --base -c --clear -dim --dimensionality -h --help -hdfs -hdfsbase -i --instance-id -jobtracker -l --list -n --namespace -p --password -resourceman -t --types -u --user -v --visibility -x --extension -z --zookeepers' -- $curr_arg ) )
            ;;
        -hdfsstage)
            COMPREPLY=( $(compgen -W '-b --base -h --help -hdfs -hdfsbase -l --list -t --types -x --extension' -- $curr_arg ) )
            ;;
        -localingest)
            COMPREPLY=( $(compgen -W '-b --base -c --clear -dim --dimensionality -h --help -i --instance-id -l --list -n --namespace -p --password -t --types -u --user -v --visibility -x --extension -z --zookeepers' -- $curr_arg ) )
            ;;
        -poststage)
            COMPREPLY=( $(compgen -W '-c --clear -dim --dimensionality -h --help -hdfs -hdfsbase -i --instance-id -jobtracker -l --list -n --namespace -p --password -resourceman -t --types -u --user -v --visibility -z --zookeepers' -- $curr_arg ) )
            ;;
    esac
    ;;
  *)
    COMPREPLY=()
    ;;
  esac
}

complete -F __geowave_ingest geowave-ingest
