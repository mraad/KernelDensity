package com.esri.density;

/**
 */
public final class CellGeoMapper extends CellMapper
{
    @Override
    protected double latitudeToY(final double lat)
    {
        return lat;
    }

    @Override
    protected double longitudeToX(final double lon)
    {
        return lon;
    }
}
