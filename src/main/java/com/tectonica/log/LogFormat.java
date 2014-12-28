package com.tectonica.log;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogFormat extends Formatter
{
	private final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

	@Override
	public String format(LogRecord record)
	{
		String dateStr = df.format(new Date(record.getMillis()));
		String levelStr = levelStrOf(record.getLevel()); // new String(record.getLevel().toString().charAt(0));
		return String.format("%s [%s] - %s%n%s", dateStr, levelStr, record.getMessage(), getExceptionString(record));
	}

	private final int TRACE2 = Level.FINEST.intValue();
	private final int TRACE1 = Level.FINER.intValue();
	private final int DEBUG = Level.FINE.intValue();
	private final int INFO = Level.INFO.intValue();
	private final int WARN = Level.WARNING.intValue();
	private final int ERROR = Level.SEVERE.intValue();

	private String levelStrOf(Level level)
	{
		int levelValue = level.intValue();
		if (levelValue == INFO)
			return "INF";
		if (levelValue == WARN)
			return "WRN";
		if (levelValue == ERROR)
			return "ERR";
		if (levelValue == DEBUG)
			return "DBG";
		if (levelValue == TRACE1)
			return "TRC";
		if (levelValue == TRACE2)
			return "TRC";
		return level.getName();
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