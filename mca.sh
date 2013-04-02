mvn -P cdh3 clean package appassembler:assemble
time sh target/appassembler/bin/KernelDensity
