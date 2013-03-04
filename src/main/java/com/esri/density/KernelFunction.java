package com.esri.density;

/**
 */
public interface KernelFunction
{
    // http://en.wikipedia.org/wiki/Uniform_kernel#Kernel_functions_in_common_use
    public double calc(final double x);
}
