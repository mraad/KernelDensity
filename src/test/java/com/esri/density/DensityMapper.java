package com.esri.density;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 */
public class DensityMapper extends AbstractMapper
{

    private final Pattern m_pattern = Pattern.compile("\t");

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        final String[] tokens = m_pattern.split(value.toString());

        final double px = Double.parseDouble(tokens[1]);
        final double py = Double.parseDouble(tokens[2]);
        final double pw = Double.parseDouble(tokens[3]);

        mapDensity(context, px, py, pw);
    }

}
