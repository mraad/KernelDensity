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
public final class CellFilterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private final static Log log = LogFactory.getLog(CellFilterMapper.class);

    private final StringBuilder m_stringBuilder = new StringBuilder();

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
        m_cell = Double.parseDouble(configuration.get("com.esri.cell", "60"));

        final String fieldSep = configuration.get("com.esri.fieldSep", ",");
        m_pattern = Pattern.compile("tab".equalsIgnoreCase(fieldSep) ? "\t" : fieldSep);

        m_numFields = Integer.parseInt(configuration.get("com.esri.numFields", "16"));
        m_lonField = Integer.parseInt(configuration.get("com.esri.lonField", "6"));
        m_latField = Integer.parseInt(configuration.get("com.esri.latField", "7"));
        m_sumField = Integer.parseInt(configuration.get("com.esri.sumField", "-1"));

        m_xmin = WebMercator.longitudeToX(m_xmin);
        m_xmax = WebMercator.longitudeToX(m_xmax);
        m_ymin = WebMercator.latitudeToY(m_ymin);
        m_ymax = WebMercator.latitudeToY(m_ymax);
    }

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

        final double px = WebMercator.longitudeToX(Double.parseDouble(lonText));
        if (px < m_xmin || px > m_xmax)
        {
            return;
        }

        final double py = WebMercator.latitudeToY(Double.parseDouble(latText));
        if (py < m_ymin || py > m_ymax)
        {
            return;
        }

        final int col = (int) Math.floor((px - m_xmin) / m_cell);
        final int row = (int) Math.floor((py - m_ymin) / m_cell);

        m_stringBuilder.setLength(0);
        m_stringBuilder.append(row).append('/').append(col);

        context.write(new Text(m_stringBuilder.toString()), new DoubleWritable(sum));
    }
}
