package org.waterme7on.hbase.ipc;

class CallEvent {

  public enum Type {
    TIMEOUT,
    CANCELLED
  }

  final Type type;

  final Call call;

  CallEvent(Type type, Call call) {
    this.type = type;
    this.call = call;
  }
}