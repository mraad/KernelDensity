package com.esri.density;

/**
 */
public final class CellGeoMapper extends CellMapper
{
    @Override
    protected double latitudeToY(final double y)
    {
        return y;
    }

    @Override
    protected double longitudeToX(final double x)
    {
        return x;
    }
}
