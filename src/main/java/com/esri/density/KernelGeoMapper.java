package com.esri.density;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 */
public class KernelGeoMapper extends KernelMapper
{
    private final Pattern m_pattern = Pattern.compile("\t");

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        final String[] tokens = m_pattern.split(value.toString());

        final double x = Double.parseDouble(tokens[0]);
        final double y = Double.parseDouble(tokens[1]);
        final double w = Double.parseDouble(tokens[2]);

        mapDensity(context, x, y, w);
    }
}
