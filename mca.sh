mvn -P cdh4 -Dmaven.test.skip=true clean package appassembler:assemble
time sh target/appassembler/bin/KernelDensity cdh4
