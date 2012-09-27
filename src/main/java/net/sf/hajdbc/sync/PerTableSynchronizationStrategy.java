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
