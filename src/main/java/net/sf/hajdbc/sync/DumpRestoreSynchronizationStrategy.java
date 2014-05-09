/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sync;

import java.io.File;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.util.Files;

/**
 * A synchronization strategy that uses dump/restore procedures.
 * @author Paul Ferraro
 */
public class DumpRestoreSynchronizationStrategy implements SynchronizationStrategy
{
	private static final long serialVersionUID = 5743532034969216540L;
	private static final String DUMP_FILE_SUFFIX = ".dump";

	private boolean dataOnly = false;

	@Override
	public String getId()
	{
		return "dump-restore";
	}

	public boolean isDataOnly()
	{
		return this.dataOnly;
	}

	public void setDataOnly(boolean dataOnly)
	{
		this.dataOnly = dataOnly;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SynchronizationStrategy#init(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> void init(DatabaseCluster<Z, D> cluster)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SynchronizationStrategy#destroy(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> void destroy(DatabaseCluster<Z, D> cluster)
	{
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.sync.SynchronizationContext)
	 */
	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context) throws SQLException
	{
		Dialect dialect = context.getDialect();
		Decoder decoder = context.getDecoder();
		DumpRestoreSupport support = dialect.getDumpRestoreSupport();
		
		if (support == null)
		{
			throw new SQLException(Messages.DUMP_RESTORE_UNSUPPORTED.getMessage(dialect));
		}
		
		try
		{
			File file = Files.createTempFile(DUMP_FILE_SUFFIX);
			
			try
			{
				support.dump(context.getSourceDatabase(), decoder, file, this.dataOnly);
				support.restore(context.getTargetDatabase(), decoder, file, this.dataOnly);
			}
			finally
			{
				Files.delete(file);
			}
		}
		catch (Exception e)
		{
			throw ExceptionType.SQL.<SQLException>getExceptionFactory().createException(e);
		}
	}
}
