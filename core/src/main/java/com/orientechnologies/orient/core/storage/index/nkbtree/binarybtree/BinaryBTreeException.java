package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.orientechnologies.orient.core.exception.ODurableComponentException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public class BinaryBTreeException extends ODurableComponentException {
  public BinaryBTreeException(BinaryBTreeException exception) {
    super(exception);
  }

  public BinaryBTreeException(String message, BinaryBTree component) {
    super(message, component);
  }
}
