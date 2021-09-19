package com.ricequant.rqutils.threadutils.pool;

/**
 * User: kangol Date: 3/25/13 Time: 10:40 PM
 */
public interface IPool<T> {

  T fetch();

  void free(T obj);

  int size();
}
