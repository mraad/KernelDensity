package com.esri.density;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

/**
 */
public class RasterAscii extends RasterAbstract
{
    @Override
    protected void writeRaster(final Map<String, Double> map) throws FileNotFoundException
    {
        final double xmin = Double.parseDouble(m_configuration.get("com.esri.xmin", "-180"));
        final double ymin = Double.parseDouble(m_configuration.get("com.esri.ymin", "-90"));
        final double xmax = Double.parseDouble(m_configuration.get("com.esri.xmax", "180"));
        final double ymax = Double.parseDouble(m_configuration.get("com.esri.ymax", "90"));
        final double cellSize = Double.parseDouble(m_configuration.get("com.esri.cellSize", "1"));
        final int ncols = (int) Math.floor((xmax - xmin) / cellSize);
        final int nrows = (int) Math.floor((ymax - ymin) / cellSize);

        final PrintWriter pw = new PrintWriter(new File(m_filePath));
        try
        {
            pw.print("NCOLS ");
            pw.println(ncols);
            pw.print("NROWS ");
            pw.println(nrows);
            pw.print("XLLCORNER ");
            pw.println(xmin);
            pw.print("YLLCORNER ");
            pw.println(ymin);
            pw.print("CELLSIZE ");
            pw.println(cellSize);
            pw.println("NODATA_VALUE 0");
            final StringBuffer stringBuffer = new StringBuffer();
            int i = nrows - 1;
            for (int r = 0; r < nrows; r++)
            {
                for (int c = 0; c < ncols; c++)
                {
                    stringBuffer.setLength(0);
                    stringBuffer.append(i).append('/').append(c);
                    final Double val = map.get(stringBuffer.toString());
                    if (val != null)
                    {
                        pw.print(val.doubleValue());
                        pw.print(' ');
                    }
                    else
                    {
                        pw.print("0 ");
                    }
                }
                i--;
                pw.println();
            }

        }
        finally
        {
            pw.close();
        }
    }

}