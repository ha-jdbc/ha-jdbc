package net.sf.hajdbc.state.distributed;

import java.io.Serializable;

import net.sf.hajdbc.durability.InvocationEvent;

public interface InvocationDescriptor extends Serializable
{
	InvocationEvent getEvent();
}
