package com.esri.density;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 */
public final class CellReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    @Override
    protected void reduce(
            final Text key,
            final Iterable<DoubleWritable> values,
            final Context context) throws IOException, InterruptedException
    {
        double sum = 0.0;
        for (final DoubleWritable doubleWritable : values)
        {
            sum += doubleWritable.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}
