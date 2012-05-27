package net.sf.hajdbc.sync;

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.TableProperties;

public interface TableSynchronizationStrategy extends Serializable
{
	<Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context, TableProperties table) throws SQLException;
	
	<Z, D extends Database<Z>> void dropConstraints(SynchronizationContext<Z, D> context) throws SQLException;
	
	<Z, D extends Database<Z>> void restoreConstraints(SynchronizationContext<Z, D> context) throws SQLException;
}
