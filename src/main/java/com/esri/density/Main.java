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
        if (args.length == 0)
        {
            context = new ClassPathXmlApplicationContext("/META-INF/spring/application-context.xml", Main.class);
        }
        else
        {
            context = new FileSystemXmlApplicationContext(args[0]);
        }
        context.registerShutdownHook();
    }
}
