package com.ricequant.rqutils.threadutils.pool;

/**
 * User: kangol Date: 3/25/13 Time: 10:40 PM
 */
public class SelfGrowObjectPool<T> implements IPool<T> {

  private final double iExpandFactor;

  private int iCurrentSize;

  private IPoolObjectFactory<T> iObjectFactory;

  private T[] iArray;

  private int iNumUsedObjects;

  private int iNextUnused;

  private int iNextUsed;

  public SelfGrowObjectPool(IPoolObjectFactory<T> factory, int initialSize) {
    this(factory, initialSize, 2);
  }

  public SelfGrowObjectPool(IPoolObjectFactory<T> factory, int initialSize, double expandFactor) {
    iObjectFactory = factory;
    iCurrentSize = initialSize;
    iExpandFactor = expandFactor;

    iArray = (T[]) new Object[initialSize];

    for (int i = 0; i < iCurrentSize; i++)
      iArray[i] = iObjectFactory.getNewObject(this);

    iNumUsedObjects = 0;
    iNextUnused = 0;
    iNextUsed = 0;
  }

  @Override
  public synchronized T fetch() {
    if (iNumUsedObjects >= iCurrentSize) {
      T[] oldArray = iArray;
      iCurrentSize *= iExpandFactor;
      iArray = (T[]) new Object[iCurrentSize];

      System.arraycopy(oldArray, 0, iArray, 0, oldArray.length);

      for (int i = oldArray.length; i < iCurrentSize; i++)
        iArray[i] = iObjectFactory.getNewObject(this);

      iNextUnused = oldArray.length;
      iNextUsed = 0;
    }

    if (iNextUnused == iCurrentSize)
      iNextUnused = 0;

    // if there is still free object, give it out
    T ret = iArray[iNextUnused];
    if (iNumUsedObjects < iCurrentSize)
      iNumUsedObjects++;

    iNextUnused++;

    return ret;
  }

  @Override
  public synchronized void free(T o) {
    if (iNumUsedObjects == 0)
      return;

    iArray[iNextUsed] = o;
    iNextUsed++;
    if (iNextUsed == iCurrentSize)
      iNextUsed = 0;

    iNumUsedObjects--;
  }

  @Override
  public synchronized int size() {
    return iCurrentSize - iNumUsedObjects;
  }
}

