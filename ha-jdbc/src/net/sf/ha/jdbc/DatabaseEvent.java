package net.sf.ha.jdbc;

import java.util.EventObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseEvent extends EventObject
{
	/**
	 * Constructs a new DatabaseEvent.
	 * @param source
	 */
	public DatabaseEvent(Database database)
	{
		super(database);
	}
	
	public Database getDatabase()
	{
		return (Database) this.getSource();
	}
}
