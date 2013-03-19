package com.esri.density;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 */
public final class Main
{
    public static void main(final String[] args) throws Exception
    {
        final ConfigurableApplicationContext context;

        if (args.length == 1)
        {
            final String arg0 = args[0];

            if (arg0.startsWith("cdh"))
            {
                final String path = "/META-INF/spring/application-context-" + arg0 + ".xml";
                context = new ClassPathXmlApplicationContext(path, Main.class);
            }
            else
            {
                context = new FileSystemXmlApplicationContext(arg0);
            }
            context.registerShutdownHook();
        }
        else
        {
            System.err.println("Missing argument cdh3|cdh4|<config>.xml");
        }
    }

}
