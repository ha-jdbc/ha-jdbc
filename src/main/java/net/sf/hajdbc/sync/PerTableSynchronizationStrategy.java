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

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

public class PerTableSynchronizationStrategy implements SynchronizationStrategy
{
	private static final long serialVersionUID = 7952995443041830678L;
	
	private final TableSynchronizationStrategy strategy;
	
	@Override
	public String getId()
	{
		return "per-table";
	}

	public PerTableSynchronizationStrategy(TableSynchronizationStrategy strategy)
	{
		this.strategy = strategy;
	}
	
	@Override
	public <Z, D extends Database<Z>> void init(DatabaseCluster<Z, D> cluster)
	{
		// Do nothing
	}

	@Override
	public <Z, D extends Database<Z>> void destroy(DatabaseCluster<Z, D> cluster)
	{
		// Do nothing
	}

	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context) throws SQLException
	{
		Connection sourceConnection = context.getConnection(context.getSourceDatabase());
		Connection targetConnection = context.getConnection(context.getTargetDatabase());
		
		SynchronizationSupport support = context.getSynchronizationSupport();
		
		this.strategy.dropConstraints(context);
		
		sourceConnection.setAutoCommit(false);
		targetConnection.setAutoCommit(false);
		
		for (TableProperties table: context.getSourceDatabaseProperties().getTables())
		{
			try
			{
				this.strategy.synchronize(context, table);
				
				targetConnection.commit();
			}
			catch (SQLException e)
			{
				support.rollback(targetConnection);
				throw e;
			}
		}
		
		this.strategy.restoreConstraints(context);
		
		support.synchronizeIdentityColumns();
		support.synchronizeSequences();
	}
}
