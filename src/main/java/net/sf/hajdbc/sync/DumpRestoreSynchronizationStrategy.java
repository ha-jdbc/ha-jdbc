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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.regex.Matcher;

import net.sf.hajdbc.ConnectionProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.util.Files;
import net.sf.hajdbc.util.Security;
import net.sf.hajdbc.util.Strings;

/**
 * A synchronization strategy that uses dump/restore procedures.
 * @author Paul Ferraro
 */
public class DumpRestoreSynchronizationStrategy implements SynchronizationStrategy
{
	private static final long serialVersionUID = 5743532034969216540L;
	private static final String DUMP_FILE_SUFFIX = ".dump";

	@Override
	public String getId()
	{
		return "dump-restore";
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
				startProcess(support.createDumpProcess(new ConnectionPropertiesImpl<Z, D>(context, context.getSourceDatabase()), file));
				
				startProcess(support.createRestoreProcess(new ConnectionPropertiesImpl<Z, D>(context, context.getTargetDatabase()), file));
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
	
	private static void startProcess(final ProcessBuilder processBuilder) throws Exception
	{
		PrivilegedExceptionAction<Process> action = new PrivilegedExceptionAction<Process>()
		{
			@Override
			public Process run() throws Exception
			{
				return processBuilder.start();
			}
		};
		
		Process process = Security.run(action);
		
		try
		{
			int status = process.waitFor();
			
			if (status != 0)
			{
				StringBuilder builder = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
				String line = reader.readLine();
				
				while (line != null)
				{
					builder.append(line).append(Strings.NEW_LINE);
					line = reader.readLine();
				}
				
				throw new Exception(builder.toString());
			}
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			
			throw new Exception(processBuilder.toString(), e);
		}
	}
	
	private static class ConnectionPropertiesImpl<Z, D extends Database<Z>> implements ConnectionProperties
	{
		private final String host;
		private final String port;
		private final String database;
		private final String user;
		private final String password;
		
		ConnectionPropertiesImpl(SynchronizationContext<Z, D> context, D database) throws SQLException
		{
			DatabaseMetaData metaData = context.getConnection(database).getMetaData();
			String url = metaData.getURL();
			
			if (url == null)
			{
				throw new UnsupportedOperationException();
			}
				
			Matcher matcher = context.getDialect().getUrlPattern().matcher(url);
			
			if (!matcher.find() || (matcher.groupCount() != 3))
			{
				throw new UnsupportedOperationException(url);
			}
			
			this.host = matcher.group(1);
			this.port = matcher.group(2);
			this.database = matcher.group(3);
			this.user = metaData.getUserName();
			this.password = database.decodePassword(context.getDecoder());
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ConnectionProperties#getHost()
		 */
		@Override
		public String getHost()
		{
			return this.host;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ConnectionProperties#getPort()
		 */
		@Override
		public String getPort()
		{
			return this.port;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ConnectionProperties#getDatabase()
		 */
		@Override
		public String getDatabase()
		{
			return this.database;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ConnectionProperties#getUser()
		 */
		@Override
		public String getUser()
		{
			return this.user;
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.ConnectionProperties#getPassword()
		 */
		@Override
		public String getPassword()
		{
			return this.password;
		}
	}
}
