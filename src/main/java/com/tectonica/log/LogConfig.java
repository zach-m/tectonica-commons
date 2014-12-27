package com.tectonica.log;

import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * Configures J.U.L by dynamically loading a <code>logging.properties</code> file from the classpath. To use:
 * <ol>
 * <li>put <code>logging.properties</code> in the <code>src/main/resources</code> folder
 * <li>add the following to your JVM arguments:
 * 
 * <pre>
 * -Djava.util.logging.config.class=com.tectonica.log.LogConfig
 * </pre>
 * 
 * </ol>
 * 
 * @author Zach Melamed
 */
public class LogConfig
{
	public LogConfig() throws Exception
	{
		try (InputStream ins = ClassLoader.getSystemResourceAsStream("logging.properties"))
		{
			LogManager.getLogManager().readConfiguration(ins);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
