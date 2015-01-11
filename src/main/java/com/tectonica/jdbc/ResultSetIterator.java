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
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tectonica.jdbc.JDBC.ExecutionContext;

/**
 * An {@link Iterator} for going over a result set row-by-row. It does not use the {@link ResultSet#isLast()} method, which isn't
 * implemented by all JDBC providers. Instead, it performs a read-ahead mechanism which is practically costless when scanning the entire
 * result set. When constructed from a {@link ExecutionContext} or from a combination of a {@link ResultSet} and {@link Connection}, it will
 * also close the associated connection after the last row has been retrieved.
 * 
 * @author Zach Melamed
 */
public class ResultSetIterator implements Iterator<ResultSet>
{
	private final ExecutionContext ctx;
	private boolean nextFound = false;

	public ResultSetIterator(ResultSet rs)
	{
		this.ctx = new ExecutionContext(null, rs);
	}

	public ResultSetIterator(ResultSet rs, Connection conn)
	{
		this.ctx = new ExecutionContext(conn, rs);
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

			// auto close connection if applicable
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