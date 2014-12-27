package com.tectonica.log;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormat extends Formatter
{
	private final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

	@Override
	public String format(LogRecord record)
	{
//		Level.FINEST  = trace()
//		Level.FINE    = debug()
//		Level.INFO    = info()
//		Level.WARNING = warn()
//		Level.SEVERE  = error()
		return String.format("%s (%s) - %s%n%s", df.format(new Date(record.getMillis())), record.getLevel().toString().charAt(0),
				record.getMessage(), getExceptionString(record));
	}

	private String getExceptionString(LogRecord record)
	{
		Throwable th = record.getThrown();
		if (th != null)
		{
			if (th.getCause() != null)
				th = th.getCause();
			StackTraceElement trace[] = th.getStackTrace();
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < trace.length; i++)
			{
				StackTraceElement frame = trace[i];
				sb.append("    ");
				sb.append(frame.getClassName()).append(".");
				sb.append(frame.getMethodName()).append("(");
				sb.append(frame.getFileName() != null ? frame.getFileName() : "").append(":");
				sb.append(frame.getLineNumber() >= 0 ? frame.getLineNumber() : "").append(")\n");
			}
			return String.format("(EXCEPTION) %s%n%s", th.toString(), sb.toString());
		}
		return "";
	}
}