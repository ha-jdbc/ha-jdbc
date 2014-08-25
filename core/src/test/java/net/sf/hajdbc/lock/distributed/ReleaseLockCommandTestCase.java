/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package net.sf.hajdbc.lock.distributed;

import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

/**
 * @author paul
 *
 */
public class ReleaseLockCommandTestCase
{
	@Test
	public void execute()
	{
		RemoteLockDescriptor descriptor = mock(RemoteLockDescriptor.class);
		LockCommandContext context = mock(LockCommandContext.class);
		Lock lock = mock(Lock.class);
		ReleaseLockCommand command = new ReleaseLockCommand(descriptor);
		Map<LockDescriptor, Lock> locks = mock(Map.class);
		
		// Existing lock
		when(context.getRemoteLocks(descriptor)).thenReturn(locks);
		when(locks.remove(descriptor)).thenReturn(lock);

		command.execute(context);
		
		verify(lock).unlock();
		
		// Non-existant lock
		when(locks.remove(descriptor)).thenReturn(null);

		command.execute(context);
		
		verifyNoMoreInteractions(lock);
	}
}
