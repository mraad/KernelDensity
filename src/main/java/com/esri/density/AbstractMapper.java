package com.esri.density;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 */
public abstract class AbstractMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private static final Log log = LogFactory.getLog(CellMapper.class);

    private final KernelFunction m_kernelFunction = new EpanechnikovKernelFunction();
    private final StringBuffer m_stringBuffer = new StringBuffer();

    private double m_xmin;
    private double m_ymin;
    private double m_xmax;
    private double m_ymax;
    private double m_cellSize;
    private double m_cellSize2;
    private double m_searchRadius;

    @Override
    protected abstract void map(
            LongWritable key,
            Text value,
            Context context) throws IOException, InterruptedException;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();
        m_xmin = Double.parseDouble(configuration.get("com.esri.xmin", "-180"));
        m_ymin = Double.parseDouble(configuration.get("com.esri.ymin", "-90"));
        m_xmax = Double.parseDouble(configuration.get("com.esri.xmax", "180"));
        m_ymax = Double.parseDouble(configuration.get("com.esri.ymax", "90"));
        m_cellSize = Double.parseDouble(configuration.get("com.esri.cellSize", "1"));
        m_cellSize2 = m_cellSize * 0.5;
        m_searchRadius = Double.parseDouble(configuration.get("com.esri.searchRadius", "1"));
        if (log.isDebugEnabled())
        {
            log.debug(String.format("%f %f %f %f", m_xmin, m_ymin, m_xmax, m_ymax));
            log.debug(String.format("cell size=%f", m_cellSize));
            log.debug(String.format("search radius=%f", m_searchRadius));
        }
    }

    protected void mapDensity(
            final Context context,
            final double px,
            final double py,
            final double pw) throws IOException, InterruptedException
    {
        double xmin = px - m_searchRadius;
        double xmax = px + m_searchRadius;
        if (xmax < m_xmin || m_xmax < xmin)
        {
            return;
        }

        double ymin = py - m_searchRadius;
        double ymax = py + m_searchRadius;
        if (ymax < m_ymin || m_ymax < ymin)
        {
            return;
        }

        xmin -= m_cellSize;
        xmax += m_cellSize;
        ymin -= m_cellSize;
        ymax += m_cellSize;

        for (double y = ymin; y < ymax; y += m_cellSize)
        {
            for (double x = xmin; x < xmax; x += m_cellSize)
            {
                final int col = (int) Math.floor((x - m_xmin) / m_cellSize);
                final int row = (int) Math.floor((y - m_ymin) / m_cellSize);
                final double cx = col * m_cellSize + m_xmin + m_cellSize2;
                if (cx < m_xmin || cx > m_xmax)
                {
                    continue;
                }
                final double cy = row * m_cellSize + m_ymin + m_cellSize2;
                if (cy < m_ymin || cy > m_ymax)
                {
                    continue;
                }
                final double dx = cx - px;
                final double dy = cy - py;
                final double rr = Math.sqrt(dx * dx + dy * dy) / m_searchRadius;
                if (rr < 1.0)
                {
                    final double w = pw * m_kernelFunction.calc(rr);
                    m_stringBuffer.setLength(0);
                    m_stringBuffer.append(row).append('/').append(col);
                    context.write(new Text(m_stringBuffer.toString()), new DoubleWritable(w));
                }
            }
        }
    }
}
