=============
KernelDensity
=============

KernelDensity analysis on Hadoop MapReduce

# Building and running

# Using CDH4 profile

    $ mvn -P cdh4 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh4


# Using CDH3 profile:

    $ mvn -P cdh3 clean package appassembler:assemble
    $ sh target/appassembler/bin/KernelDensity cdh3
