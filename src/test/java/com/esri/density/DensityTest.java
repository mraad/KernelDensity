package com.esri.density;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DensityTest
{
    private MapDriver<
            LongWritable, Text,
            LongWritable, DoubleWritable
            > m_mapDriver;
    private ReduceDriver<
            LongWritable, DoubleWritable,
            LongWritable, DoubleWritable
            > m_reduceDriver;
    private MapReduceDriver<
            LongWritable, Text,
            LongWritable, DoubleWritable,
            LongWritable, DoubleWritable
            > m_mapReduceDriver;

    private Configuration m_configuration;

    private KernelFunction m_kernelFunction = new EpanechnikovKernelFunction();

    private double toWeight(
            final double px,
            final double py,
            final double cx,
            final double cy,
            final double sr)
    {
        final double dx = px - cx;
        final double dy = py - cy;
        final double rr = Math.sqrt(dx * dx + dy * dy) / sr;
        return m_kernelFunction.calc(rr);
    }

    private LongWritable toKey(
            final long row,
            final long col)
    {
        return new LongWritable((row << 32) | col);
    }

    @Before
    public void setUp()
    {
        m_configuration = new Configuration();
        m_configuration.set("com.esri.xmin", "-180");
        m_configuration.set("com.esri.ymin", "-90");
        m_configuration.set("com.esri.xmax", "180");
        m_configuration.set("com.esri.ymax", "90");

        final AbstractMapper mapper = new DensityMapper();

        m_mapDriver = new MapDriver<LongWritable, Text, LongWritable, DoubleWritable>();
        m_mapDriver.setMapper(mapper);
        m_mapDriver.setConfiguration(m_configuration);

        final CellReducer reducer = new CellReducer();

        m_reduceDriver = new ReduceDriver<LongWritable, DoubleWritable, LongWritable, DoubleWritable>();
        m_reduceDriver.setReducer(reducer);
        m_reduceDriver.setConfiguration(m_configuration);

        m_mapReduceDriver = new MapReduceDriver<
                LongWritable, Text,
                LongWritable, DoubleWritable,
                LongWritable, DoubleWritable>();
        m_mapReduceDriver.setMapper(mapper);
        m_mapReduceDriver.setReducer(reducer);
        m_mapReduceDriver.setConfiguration(m_configuration);
    }

    @Test
    public void testMapperOne() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final String[] tokens = new String[]{
                "1", // id
                "0", // x
                "0", // y
                "1" // w
        };
        m_mapDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(toKey(89, 179), new DoubleWritable(toWeight(-0.5, -0.5, 0.0, 0.0, 1)));
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, 0.0, 0.0, 1)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, 0.0, 0.0, 1)));
        m_mapDriver.addOutput(toKey(90, 180), new DoubleWritable(toWeight(+0.5, +0.5, 0.0, 0.0, 1)));
        m_mapDriver.runTest();
    }

    @Test
    public void testMapperEdge1SearchRadius() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final String[] tokens = new String[]{
                "1", // id
                "0.1", // x
                "0.1", // y
                "1" // w
        };
        m_mapDriver.withInput(new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(toKey(89, 179), new DoubleWritable(toWeight(-0.5, -0.5, 0.1, 0.1, 1)));
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, 0.1, 0.1, 1)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, 0.1, 0.1, 1)));
        m_mapDriver.addOutput(toKey(90, 180), new DoubleWritable(toWeight(+0.5, +0.5, 0.1, 0.1, 1)));
        m_mapDriver.runTest();
    }

    @Test
    public void testMapperEdge2() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final String[] tokens = new String[]{
                "1", // id
                "-0.1", // x
                "-0.1", // y
                "1" // w
        };
        m_mapDriver.withInput(new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(toKey(89, 179), new DoubleWritable(toWeight(-0.5, -0.5, -0.1, -0.1, 1)));
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, -0.1, -0.1, 1)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, -0.1, -0.1, 1)));
        m_mapDriver.addOutput(toKey(90, 180), new DoubleWritable(toWeight(+0.5, +0.5, -0.1, -0.1, 1)));
        m_mapDriver.runTest();
    }

    @Test
    public void testMapperEdge3() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final String[] tokens = new String[]{
                "1", // id
                "0.4", // x
                "0.4", // y
                "1" // w
        };
        m_mapDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, 0.4, 0.4, 1)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, 0.4, 0.4, 1)));
        m_mapDriver.addOutput(toKey(90, 180), new DoubleWritable(toWeight(+0.5, +0.5, 0.4, 0.4, 1)));
        m_mapDriver.runTest();
    }

    @Test
    public void testMapperEdge4() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final double x = -0.4;
        final double y = -0.4;
        final String[] tokens = new String[]{
                "1", // id
                Double.toString(x), // x
                Double.toString(y), // y
                "1" // w
        };
        m_mapDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(toKey(89, 179), new DoubleWritable(toWeight(-0.5, -0.5, x, y, 1)));
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, x, y, 1)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, x, y, 1)));
        m_mapDriver.runTest();
    }

    @Test
    public void testMapperTwo() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "2");

        final double x = 0.0;
        final double y = 0.0;
        final String[] tokens = new String[]{
                "1", // id
                Double.toString(x), // x
                Double.toString(y), // y
                "1" // w
        };
        m_mapDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapDriver.resetOutput();

        m_mapDriver.addOutput(toKey(88, 179), new DoubleWritable(toWeight(-0.5, -1.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(88, 180), new DoubleWritable(toWeight(+0.5, -1.5, x, y, 2)));

        m_mapDriver.addOutput(toKey(89, 178), new DoubleWritable(toWeight(-1.5, -0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(89, 179), new DoubleWritable(toWeight(-0.5, -0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(89, 180), new DoubleWritable(toWeight(+0.5, -0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(89, 181), new DoubleWritable(toWeight(+1.5, -0.5, x, y, 2)));

        m_mapDriver.addOutput(toKey(90, 178), new DoubleWritable(toWeight(-1.5, +0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(90, 179), new DoubleWritable(toWeight(-0.5, +0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(90, 180), new DoubleWritable(toWeight(+0.5, +0.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(90, 181), new DoubleWritable(toWeight(+1.5, +0.5, x, y, 2)));

        m_mapDriver.addOutput(toKey(91, 179), new DoubleWritable(toWeight(-0.5, +1.5, x, y, 2)));
        m_mapDriver.addOutput(toKey(91, 180), new DoubleWritable(toWeight(+0.5, +1.5, x, y, 2)));

        m_mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException
    {
        final List<DoubleWritable> list = new ArrayList<DoubleWritable>();
        list.add(new DoubleWritable(1));
        list.add(new DoubleWritable(2));
        list.add(new DoubleWritable(3));

        final LongWritable rowcol = new LongWritable();
        m_reduceDriver.withInput(rowcol, list);
        m_reduceDriver.withOutput(rowcol, new DoubleWritable(6));
        m_reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException
    {
        m_configuration.set("com.esri.cellSize", "1");
        m_configuration.set("com.esri.searchRadius", "1");

        final double x = 0.0;
        final double y = 0.0;
        final String[] tokens = new String[]{
                "1", // id
                Double.toString(x), // x
                Double.toString(y), // y
                "1" // w
        };
        final double w = toWeight(0.5, 0.5, x, y, 1);
        m_mapReduceDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join(tokens, '\t')));
        m_mapReduceDriver.resetOutput();
        m_mapReduceDriver.addOutput(toKey(89, 179), new DoubleWritable(w));
        m_mapReduceDriver.addOutput(toKey(89, 180), new DoubleWritable(w));
        m_mapReduceDriver.addOutput(toKey(90, 179), new DoubleWritable(w));
        m_mapReduceDriver.addOutput(toKey(90, 180), new DoubleWritable(w));
        m_mapReduceDriver.runTest();
    }

}
