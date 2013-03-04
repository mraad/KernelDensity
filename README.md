KernelDensity
=============

[KernelDensity analysis on Hadoop MapReduce] (http://thunderheadxpler.blogspot.com/2013/03/bigdata-kernel-density-analysis-on.html "KernelDensity analysis on Hadoop MapReduce")

## Loading data

You can create a sample set to test the process:

    $ awk -f data.awk > /tmp/data.tsv
    $ hadoop fs -mkdir input
    $ hadoop fs -put /tmp/data.tsv input

## Building and running

Make sure to check the parameters of the application-context-xxx.xml files before building and running

## Using CDH4 profile

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh4


## Using CDH3 profile:

    $ mvn -P cdh3 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh3
