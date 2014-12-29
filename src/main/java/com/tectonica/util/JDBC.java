package com.tectonica.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.sql.DataSource;

public class JDBC
{
	public static abstract class ConnListener<T>
	{
		protected abstract T onConnection(final Connection conn) throws SQLException;

		@SuppressWarnings("unchecked")
		protected T readSingle(ResultSet rs) throws SQLException
		{
			if (rs.next())
				return (T) rs.getObject(1);
			return null;
		}

		@SuppressWarnings("unchecked")
		protected <V> V readSingle(ResultSet rs, Class<V> clz) throws SQLException
		{
			if (rs.next())
				return (V) rs.getObject(1);
			return null;
		}
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

		private ExecutionContext(Connection conn, ResultSet rs)
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

	public static class ResultSetIterator implements Iterator<ResultSet>
	{
		private final ExecutionContext ctx;
		private boolean nextFound = false;

		public ResultSetIterator(ResultSet rs)
		{
			this.ctx = new ExecutionContext(null, rs);
		}

		public ResultSetIterator(ExecutionContext ctx)
		{
			this.ctx = ctx;
		}

		@Override
		public boolean hasNext()
		{
			if (!nextFound)
			{
				try
				{
					nextFound = ctx.rs.next();
				}
				catch (SQLException e)
				{
					throw new RuntimeException(e);
				}
				if (!nextFound && (ctx.conn != null))
				{
					try
					{
						ctx.conn.close();
					}
					catch (SQLException e)
					{
						e.printStackTrace(); // not a critical error
					}
				}
			}
			return nextFound;
		}

		@Override
		public ResultSet next()
		{
			if (!hasNext())
				throw new NoSuchElementException();
			nextFound = false;
			return ctx.rs;
		}

		@Override
		public void remove()
		{
			try
			{
				ctx.rs.deleteRow();
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e);
			}
		}
	}
}
