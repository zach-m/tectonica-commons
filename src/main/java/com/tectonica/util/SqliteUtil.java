package com.tectonica.util;

import javax.sql.DataSource;

import org.sqlite.SQLiteDataSource;

public class SqliteUtil
{
	public static JDBC connect(String connStr)
	{
		try
		{
			Class.forName("org.sqlite.JDBC");
		}
		catch (ClassNotFoundException e)
		{
			throw new RuntimeException(e);
		}
		DataSource connPool = new SQLiteDataSource();
		((SQLiteDataSource) connPool).setUrl(connStr);

		return new JDBC(connPool);
	}
}
