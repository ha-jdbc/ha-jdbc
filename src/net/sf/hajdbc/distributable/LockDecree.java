package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.LockManager;

import org.jgroups.Address;

public interface LockDecree extends Serializable
{
	/**
	 * Returns the address of the node that requested the lock
	 * @return a node address
	 */
	public Address getAddress();

	/**
	 * Execute lock operation.
	 * @param lockManager a lock manager
	 * @param lockMap a map of object identifier to lock
	 * @return true if operation was successful, false otherwise.
	 */
	public boolean vote(LockManager lockManager, Map<String, Lock> lockMap);
}