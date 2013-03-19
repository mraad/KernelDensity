package com.esri.density;

/**
 */
public final class NumeUtil
{
    /**
     * Naive implementation
     *
     * @param s text
     * @return true/false
     */
    static boolean isDouble(final String s)
    {
        if (s == null)
        {
            return false;
        }
        final int len = s.length();
        if (len == 0)
        {
            return false;
        }
        boolean cond = true;
        for (int i = 0; i < len; i++)
        {
            final Character c = s.charAt(i);
            if (c == '-')
            {
                continue;
            }
            if (c == '.')
            {
                continue;
            }
            if (Character.isDigit(c))
            {
                continue;
            }
            if (Character.isSpaceChar(c))
            {
                continue;
            }
            cond = false;
            break;
        }
        return cond;
    }
}
