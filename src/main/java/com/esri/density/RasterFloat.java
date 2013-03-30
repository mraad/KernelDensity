package com.esri.density;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 */
public final class RasterFloat extends RasterAbstract
{
    @Override
    protected void writeRaster(
            final Map<Long, Double> map,
            final double xmin,
            final double ymin,
            final int ncols,
            final int nrows,
            final double cell) throws IOException
    {
        writeHeader(xmin, ymin, cell, ncols, nrows);
        writeFloat(map, ncols, nrows);

    }

    private void writeFloat(
            final Map<Long, Double> map,
            final int ncols,
            final int nrows) throws IOException
    {
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(m_filePath)));
        try
        {
            long i = nrows - 1;
            for (int r = 0; r < nrows; r++)
            {
                final long row = i << 32;
                for (int c = 0; c < ncols; c++)
                {
                    final Double val = map.get(row | c);
                    if (val != null)
                    {
                        dos.writeFloat(val.floatValue());
                    }
                    else
                    {
                        dos.writeFloat(0);
                    }
                }
                i--;
            }
        }
        finally
        {
            dos.close();
        }
    }

    private void writeHeader(
            final double xmin,
            final double ymin,
            final double cellSize,
            final int ncols,
            final int nrows) throws FileNotFoundException
    {
        final PrintWriter pw = new PrintWriter(new File(m_filePath.replaceFirst(".flt", ".hdr")));
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
            pw.println("BYTEORDER MSBFIRST");
        }
        finally
        {
            pw.close();
        }
    }
}
