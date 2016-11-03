package com.datatorrent.example.utils;


import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.plan.logical.DefaultKryoStreamCodec;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class JavaSerializationStreamCodec<T> extends DefaultKryoStreamCodec<T> {

  private static final long serialVersionUID = -183071548840076388L;

  public JavaSerializationStreamCodec() {
   super();
   this.kryo.setDefaultSerializer(JavaSerializer.class);
  }

  @Override
  public Slice toByteArray(T info) {
    return super.toByteArray(info);
  }

  @Override
  public Object fromByteArray(Slice fragment) {
    return super.fromByteArray(fragment);
  }
}
