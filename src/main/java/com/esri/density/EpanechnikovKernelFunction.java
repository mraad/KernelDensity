package com.esri.density;

/**
 */
public final class EpanechnikovKernelFunction implements KernelFunction
{
    @Override
    public double calc(final double x)
    {
        return 3.0 * (1.0 - x * x) / 4.0;
    }
}
