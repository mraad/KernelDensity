package com.esri.density;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 */
public class Main
{
    public static void main(final String[] args) throws Exception
    {
        final String suffix = args.length == 0 ? "cdh4" : args[0];
        final String path = "/META-INF/spring/application-context-" + suffix + ".xml";

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext(path, Main.class);

        context.registerShutdownHook();
    }

}
