/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.jdbc;

import java.util.Date;

import javax.sql.DataSource;

import org.sqlite.SQLiteDataSource;

/**
 * Generator for SQLite Connection Pool. Requires:
 * 
 * <pre>
 * &lt;dependency&gt;
 *     &lt;groupId&gt;org.xerial&lt;/groupId&gt;
 *     &lt;artifactId&gt;sqlite-jdbc&lt;/artifactId&gt;
 * &lt;/dependency&gt;
 * </pre>
 * 
 * @author Zach Melamed
 */
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

	public static <F> String javaTypeToSqliteTypeAffinity(Class<F> indexClz)
	{
		String colType = "NONE";
		if (indexClz != null)
		{
			if (CharSequence.class.isAssignableFrom(indexClz))
				colType = "TEXT";
			else if (Number.class.isAssignableFrom(indexClz))
			{
				colType = "NUMERIC";
				if (indexClz == Byte.class || indexClz == Short.class || indexClz == Integer.class || indexClz == Long.class)
					colType = "INTEGER";
				else if (indexClz == Float.class || indexClz == Double.class)
					colType = "REAL";
			}
			else if (Date.class.isAssignableFrom(indexClz))
				colType = "DATETIME"; // not an affinity, yet clearer
		}
		return colType;
	}

}
