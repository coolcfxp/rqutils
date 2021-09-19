package com.ricequant.rqutils.threadutils.pool;

/**
 * User: kangol Date: 3/26/13 Time: 9:25 PM
 */
public interface IPoolObjectFactory<T> {

  T getNewObject(IPool<T> pool);
}
