mvn -P cdh4 clean package appassembler:assemble
time sh target/appassembler/bin/KernelDensity
