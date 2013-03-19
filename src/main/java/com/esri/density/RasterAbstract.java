package com.esri.density;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 */
public abstract class RasterAbstract implements Callable
{
    protected static final Log log = LogFactory.getLog(RasterFloat.class);

    protected Configuration m_configuration;
    protected String m_hdfsPath;
    protected String m_filePath;

    public void setConfiguration(Configuration configuration)
    {
        m_configuration = configuration;
    }

    public void setHdfsPath(final String hdfsPath)
    {
        m_hdfsPath = hdfsPath;
    }

    public void setFilePath(final String filePath)
    {
        m_filePath = filePath;
    }

    @Override
    public Object call() throws Exception
    {
        final double xmin = WebMercator.longitudeToX(Double.parseDouble(m_configuration.get("com.esri.xmin", "-180")));
        final double xmax = WebMercator.longitudeToX(Double.parseDouble(m_configuration.get("com.esri.xmax", "180")));
        final double ymin = WebMercator.latitudeToY(Double.parseDouble(m_configuration.get("com.esri.ymin", "-90")));
        final double ymax = WebMercator.latitudeToY(Double.parseDouble(m_configuration.get("com.esri.ymax", "90")));
        final double cell = Double.parseDouble(m_configuration.get("com.esri.cell", "60"));

        final int ncols = (int) Math.floor((xmax - xmin) / cell);
        final int nrows = (int) Math.floor((ymax - ymin) / cell);

        final Map<String, Double> map = loadMapFromHDFS();
        writeRaster(map, xmin, ymin, ncols, nrows, cell);
        return null;
    }

    private Map<String, Double> loadMapFromHDFS() throws IOException
    {
        final Map<String, Double> map = new HashMap<String, Double>();
        final String defaultFS = getDefaultFS();
        final Path path = new Path(defaultFS + m_hdfsPath);
        final FileSystem fileSystem = path.getFileSystem(m_configuration);
        final FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (fileStatus.isDir()) // use isDirectory() for CDH4
        {
            for (final FileStatus childStatus : fileSystem.listStatus(path))
            {
                final Path childPath = childStatus.getPath();
                if (!childPath.getName().startsWith("_"))
                {
                    if (log.isDebugEnabled())
                    {
                        log.debug(childPath.getName());
                    }
                    final FSDataInputStream dataInputStream = fileSystem.open(childPath);
                    try
                    {
                        final LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(dataInputStream));
                        for (String line = lineNumberReader.readLine(); line != null; line = lineNumberReader.readLine())
                        {
                            final int index = line.indexOf('\t');
                            final String key = line.substring(0, index);
                            final String val = line.substring(index + 1);
                            map.put(key, Double.parseDouble(val));
                        }
                    }
                    finally
                    {
                        dataInputStream.close();
                    }
                }
            }
        }
        return map;
    }

    private String getDefaultFS()
    {
        final String val = m_configuration.get("fs.defaultFS");
        return val == null ? m_configuration.get("fs.default.name") : val;
    }

    protected abstract void writeRaster(
            Map<String, Double> map,
            final double xmin,
            final double ymin,
            final int ncols,
            final int nrows,
            final double cell) throws IOException;
}