package com.esri.density;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 */
public abstract class CellMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable>
{
    private final static Log log = LogFactory.getLog(CellMapper.class);

    private double m_xmin;
    private double m_ymin;
    private double m_xmax;
    private double m_ymax;
    private double m_cell;
    private int m_numFields;
    private int m_lonField;
    private int m_latField;
    private int m_sumField;
    private Pattern m_pattern;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();

        m_xmin = Double.parseDouble(configuration.get("com.esri.xmin", "-180"));
        m_ymin = Double.parseDouble(configuration.get("com.esri.ymin", "-90"));
        m_xmax = Double.parseDouble(configuration.get("com.esri.xmax", "180"));
        m_ymax = Double.parseDouble(configuration.get("com.esri.ymax", "90"));
        m_cell = Double.parseDouble(configuration.get("com.esri.cellSize", "1"));

        final String fieldSep = configuration.get("com.esri.fieldSep", ",");
        m_pattern = Pattern.compile("tab".equalsIgnoreCase(fieldSep) ? "\t" : fieldSep);

        m_numFields = Integer.parseInt(configuration.get("com.esri.numFields", "3"));
        m_lonField = Integer.parseInt(configuration.get("com.esri.lonField", "0"));
        m_latField = Integer.parseInt(configuration.get("com.esri.latField", "1"));
        m_sumField = Integer.parseInt(configuration.get("com.esri.sumField", "2"));

        m_xmin = longitudeToX(m_xmin);
        m_xmax = longitudeToX(m_xmax);
        m_ymin = latitudeToY(m_ymin);
        m_ymax = latitudeToY(m_ymax);
    }

    protected abstract double latitudeToY(final double ymin);

    protected abstract double longitudeToX(final double xmin);

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        final String[] tokens = m_pattern.split(value.toString());
        if (tokens.length != m_numFields)
        {
            return;
        }
        final String lonText = tokens[m_lonField];
        if (!DoubleUtil.isDouble(lonText))
        {
            return;
        }
        final String latText = tokens[m_latField];
        if (!DoubleUtil.isDouble(latText))
        {
            return;
        }

        final String sumText = tokens[m_sumField];
        if (!DoubleUtil.isDouble(sumText))
        {
            return;
        }
        final double sum = Double.parseDouble(sumText);
        if (sum == 0.0)
        {
            return;
        }

        final double px = longitudeToX(Double.parseDouble(lonText));
        if (px < m_xmin || px > m_xmax)
        {
            return;
        }

        final double py = latitudeToY(Double.parseDouble(latText));
        if (py < m_ymin || py > m_ymax)
        {
            return;
        }

        final long col = (long) Math.floor((px - m_xmin) / m_cell) & 0x7FFFL;
        final long row = (long) Math.floor((py - m_ymin) / m_cell) & 0x7FFFL;

        context.write(new LongWritable((row << 32) | col), new DoubleWritable(sum));
    }
}
