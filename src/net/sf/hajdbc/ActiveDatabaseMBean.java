/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.util.Properties;

/**
 * @author Paul Ferraro
 *
 */
public interface ActiveDatabaseMBean
{
	/**
	 * Returns the unique idenfier for this database
	 * @return a unique identifier
	 */
	public String getId();
	
	/**
	 * Returns the relative "weight" of this cluster node.
	 * In general, when choosing a node to service read requests, a cluster will favor the node with the highest weight.
	 * A weight of 0 is somewhat special, depending on the type of balancer used by the cluster.
	 * In most cases, a weight of 0 means that this node will never service read requests unless it is the only node in the cluster.
	 * @return a positive integer
	 */
	public int getWeight();
	
	/**
	 * @return the database user
	 */
	public String getUser();
	
	/**
	 * @return the password of the database user
	 */
	public String getPassword();
	
	/**
	 * Returns a collection of additional properties of this database.
	 * @return additional properties of this database
	 */
	public Properties getProperties();
}
