package net.sf.hajdbc.lock.distributed;

import java.io.Serializable;

public interface LockDescriptor extends Serializable
{
	/**
	 * @return the id
	 */
	String getId();

	/**
	 * @return the type
	 */
	LockType getType();
}
