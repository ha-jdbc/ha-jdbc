package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class SavepointProxy implements Savepoint
{
	private Map savepointMap;
	
	public SavepointProxy(Map savepointMap)
	{
		this.savepointMap = savepointMap;
	}
	
	/**
	 * @see java.sql.Savepoint#getSavepointId()
	 */
	public int getSavepointId() throws SQLException
	{
		return this.getSavepoint().getSavepointId();
	}

	/**
	 * @see java.sql.Savepoint#getSavepointName()
	 */
	public String getSavepointName() throws SQLException
	{
		return this.getSavepoint().getSavepointName();
	}
	
	public Savepoint getSavepoint()
	{
		return (Savepoint) this.savepointMap.values().iterator().next();
	}
	
	public Map getSavepointMap()
	{
		return this.savepointMap;
	}
}
