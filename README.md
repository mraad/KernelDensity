=============
KernelDensity
=============

[KernelDensity analysis on Hadoop MapReduce] (http://thunderheadxpler.blogspot.com/2013/03/bigdata-kernel-density-analysis-on.html "KernelDensity analysis on Hadoop MapReduce")

## Building and running

## Using CDH4 profile

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh4


## Using CDH3 profile:

    $ mvn -P cdh3 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh3
