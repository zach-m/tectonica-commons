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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Simple wrapper for basic boilerplate JDBC tasks
 * 
 * @author Zach Melamed
 */
public class JDBC
{
	public static interface ConnListener<T>
	{
		T onConnection(final Connection conn) throws SQLException;
	}

	protected final DataSource connPool;

	public JDBC(DataSource connPool)
	{
		this.connPool = connPool;
	}

	public <T> T execute(ConnListener<T> connListener)
	{
		Connection conn = null;
		try
		{
			conn = connPool.getConnection();
			try
			{
				return connListener.onConnection(conn);
			}
			finally
			{
				conn.close();
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public <T> T transact(ConnListener<T> connListener)
	{
		Connection conn = null;
		try
		{
			conn = connPool.getConnection();
			try
			{
				conn.setAutoCommit(false);
				T retVal = connListener.onConnection(conn);
				conn.commit();
				return retVal;
			}
			finally
			{
				conn.close();
			}
		}
		catch (SQLException e)
		{
			try
			{
				if (conn != null)
					conn.rollback();
			}
			catch (SQLException e1)
			{
			}
			throw new RuntimeException(e);
		}
	}

	public static class ExecutionContext
	{
		public final Connection conn;
		public final ResultSet rs;

		ExecutionContext(Connection conn, ResultSet rs)
		{
			this.conn = conn;
			this.rs = rs;
		}
	}

	public ExecutionContext startExecute(ConnListener<ResultSet> connListener)
	{
		Connection conn = null;
		try
		{
			conn = connPool.getConnection();
			ResultSet rs = connListener.onConnection(conn);
			return new ExecutionContext(conn, rs);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public ExecutionContext startTransact(ConnListener<ResultSet> connListener)
	{
		Connection conn = null;
		try
		{
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ResultSet rs = connListener.onConnection(conn);
			return new ExecutionContext(conn, rs);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void endTransact(ExecutionContext ctx, boolean commit)
	{
		try
		{
			try
			{
				if (commit)
					ctx.conn.commit();
				else
					ctx.conn.rollback();
			}
			finally
			{
				ctx.conn.close();
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}
}
