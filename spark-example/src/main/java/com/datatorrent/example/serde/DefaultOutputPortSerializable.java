package com.datatorrent.example.serde;

import java.io.Serializable;

import com.datatorrent.api.DefaultOutputPort;

public class DefaultOutputPortSerializable<T> extends DefaultOutputPort<T> implements Serializable
{
}
