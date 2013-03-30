package com.esri.density;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Map;

/**
 */
public final class RasterAscii extends RasterAbstract
{
    @Override
    protected void writeRaster(
            final Map<Long, Double> map,
            final double xmin,
            final double ymin,
            final int ncols,
            final int nrows,
            final double cell) throws FileNotFoundException
    {
        final PrintWriter pw = new PrintWriter(new BufferedOutputStream(new FileOutputStream(m_filePath)));
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
            pw.println(cell);
            pw.println("NODATA_VALUE 0");
            long i = nrows - 1;
            for (int r = 0; r < nrows; r++)
            {
                final long row = i << 32;
                for (int c = 0; c < ncols; c++)
                {
                    final Double val = map.get(row | c);
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