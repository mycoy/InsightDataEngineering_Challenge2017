#!/usr/bin/env bash

# one example of run.sh script for implementing the features using python
# the contents of this script could be replaced with similar files from any major language

# I'll execute my programs, with the input directory log_input and output the files in the directory log_output
#python ./src/process_log.py ./log_input/log.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt

current_path=`pwd`
inputFile=$current_path/log_input/log.txt
outputF1=$current_path/log_output/hosts.txt
outputF2=$current_path/log_output/resources.txt
outputF3=$current_path/log_output/hours.txt 
outputF4=$current_path/log_output/blocked.txt 

cd ./src
javac *.java
java ProcessLog  $inputFile  $outputF1  $outputF2  $outputF3  $outputF4
cd ..

