#!/bin/bash

for i in 40
do
    for j in majority,majority 1,3; 
    do 
        IFS=","
        set -- $j;"
        java -cp target/*:target/dependency/*:. assign2.Experiment $i $1 $2
    done
done
