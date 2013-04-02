package com.esri.density;

/**
 */
public final class WebMercator
{
    public static final double RADIANS_PER_DEGREES = Math.PI / 180.0;
    public static final double RADIUS = 6378137.0;

    public static double latitudeToY(double latitude)
    {
        final double rad = latitude * RADIANS_PER_DEGREES;
        final double sin = Math.sin(rad);
        return RADIUS * 0.5 * Math.log((1.0 + sin) / (1.0 - sin));
    }

    public static double longitudeToX(double longitude)
    {
        return longitude * RADIANS_PER_DEGREES * RADIUS;
    }
}
