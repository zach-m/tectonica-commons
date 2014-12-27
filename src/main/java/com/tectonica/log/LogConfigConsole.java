package com.tectonica.log;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.logging.ConsoleHandler;
import java.util.logging.LogManager;

/**
 * Configures J.U.L to print to the console using a Tectonica formatter. To use:
 * <ol>
 * <li>add the following to your JVM arguments:
 * 
 * <pre>
 * -Djava.util.logging.config.class=com.tectonica.log.LogConfigConsole
 * </pre>
 * 
 * <li>optionally, if you want to state the minimum log level, add another argument, like this:
 * 
 * <pre>
 * -Dcom.tectonica.log.level=FINEST
 * </pre>
 * 
 * <li>optionally, if you want to provide your own formatter, add another argument, like this:
 * 
 * <pre>
 * -Dcom.tectonica.log.formatter=com.tectonica.log.LogFormat
 * </pre>
 * 
 * </ol>
 * 
 * @author Zach Melamed
 */
public class LogConfigConsole
{
	private static final String DEFAULT_LEVEL = "INFO";
	private static final String DEFAULT_FORMATTER = LogFormat.class.getName();

	public LogConfigConsole() throws Exception
	{
		String level = System.getProperty("com.tectonica.log.level");
		if (level == null || level.isEmpty())
			level = DEFAULT_LEVEL;

		String formatterClassName = System.getProperty("com.tectonica.log.formatter");
		if (formatterClassName == null || formatterClassName.isEmpty())
			formatterClassName = DEFAULT_FORMATTER;

		StringBuilder props = new StringBuilder();

		// configure globals
		props.append(".level = ").append(level).append("\n");

		// configure handlers
		String con = ConsoleHandler.class.getName();
		props.append("handlers = ").append(con).append("\n");

		// configure console handler
		props.append(con).append(".level = ").append(level).append("\n");
		props.append(con).append(".formatter = ").append(formatterClassName).append("\n");

		// apply configuration
		try (InputStream ins = new ByteArrayInputStream(props.toString().getBytes()))
		{
			LogManager.getLogManager().readConfiguration(ins);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
