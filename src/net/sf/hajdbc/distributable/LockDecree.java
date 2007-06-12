package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.LockManager;

import org.jgroups.Address;

public interface LockDecree extends Serializable
{
	public String getId();

	public Address getAddress();

	/**
	 * Execute lock operation.
	 * @param lockManager a lock manager
	 * @param lockMap a map of decree to lock
	 * @return true if operation was successful, false otherwise.
	 */
	public boolean vote(LockManager lockManager, Map<LockDecree, Lock> lockMap);
}