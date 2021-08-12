package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.orientechnologies.common.comparator.OComparatorFactory;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

public class BinaryBTreeTestIT {
  private OAtomicOperationsManager atomicOperationsManager;
  private BinaryBTree binaryBTree;
  private OrientDB orientDB;

  private String dbName;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.setValue(4);
    OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.setValue(1024);

    final String buildDirectory =
        System.getProperty("buildDirectory", ".")
            + File.separator
            + BinaryBTree.class.getSimpleName();

    dbName = "binaryBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    final OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE, 4)
            .addConfig(OGlobalConfiguration.SBTREE_MAX_KEY_SIZE, 1024)
            .build();

    orientDB = new OrientDB("plocal:" + buildDirectory, config);
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    OAbstractPaginatedStorage storage;
    try (ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin")) {
      storage =
          (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
    }
    binaryBTree = new BinaryBTree(1, 1024, 16, storage, "singleBTree", ".bbt");
    atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> binaryBTree.create(atomicOperation));
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() throws Exception {
    final int keysCount = 1_000_000;

    final int rollbackInterval = 100;
    String[] lastKey = new String[1];
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final String key = Integer.toString(iterationCounter * rollbackInterval + j);
                  binaryBTree.put(
                      atomicOperation,
                      key.getBytes(StandardCharsets.UTF_8),
                      new ORecordId(
                          (iterationCounter * rollbackInterval + j) % 32000,
                          iterationCounter * rollbackInterval + j));

                  if (rollbackCounter == 1) {
                    if ((iterationCounter * rollbackInterval + j) % 100_000 == 0) {
                      System.out.printf(
                          "%d items loaded out of %d%n",
                          iterationCounter * rollbackInterval + j, keysCount);
                    }

                    if (lastKey[0] == null) {
                      lastKey[0] = key;
                    } else if (key.compareTo(lastKey[0]) > 0) {
                      lastKey[0] = key;
                    }
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      //      Assert.assertEquals("0", binaryBTree.firstKey());
      //      Assert.assertEquals(lastKey[0], binaryBTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(
          i + " key is absent",
          new ORecordId(i % 32000, i),
          binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d%n", i, keysCount);
      }
    }
    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertNull(binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<byte[]> keys =
        new TreeSet<>(OComparatorFactory.INSTANCE.getComparator(byte[].class));
    final Random random = new Random();
    final int keysCount = 1_000_000;

    final int rollbackRange = 100;
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);
                  binaryBTree.put(
                      atomicOperation,
                      key.getBytes(StandardCharsets.UTF_8),
                      new ORecordId(val % 32000, val));

                  if (rollbackCounter == 1) {
                    keys.add(key.getBytes(StandardCharsets.UTF_8));
                  }
                  Assert.assertEquals(
                      binaryBTree.get(key.getBytes(StandardCharsets.UTF_8)),
                      new ORecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    //    Assert.assertEquals(binaryBTree.firstKey(), keys.first());
    //    Assert.assertEquals(binaryBTree.lastKey(), keys.last());
    for (byte[] key : keys) {
      final int val = Integer.parseInt(new String(key, StandardCharsets.UTF_8));
      Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    final NavigableSet<byte[]> keys =
        new TreeSet<>(OComparatorFactory.INSTANCE.getComparator(byte[].class));
    long seed = System.currentTimeMillis();
    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 1_000_000;
    final int rollbackRange = 100;

    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val;
                  do {
                    val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                  } while (val < 0);

                  final byte[] key = Integer.toString(val).getBytes(StandardCharsets.UTF_8);
                  binaryBTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keys.add(key);
                  }
                  Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }
    //    Assert.assertEquals(binaryBTree.firstKey(), keys.first());
    //    Assert.assertEquals(binaryBTree.lastKey(), keys.last());

    for (byte[] key : keys) {
      int val = Integer.parseInt(new String(key, StandardCharsets.UTF_8));
      Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    final int keysCount = 1_000_000;

    final NavigableSet<byte[]> keys =
        new TreeSet<>(OComparatorFactory.INSTANCE.getComparator(byte[].class));
    for (int i = 0; i < keysCount; i++) {
      final byte[] key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
      final int k = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> binaryBTree.put(atomicOperation, key, new ORecordId(k % 32000, k)));
      keys.add(key);
    }

    Iterator<byte[]> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      final byte[] key = keysIterator.next();
      if (Integer.parseInt(new String(key, StandardCharsets.UTF_8)) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> binaryBTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    // Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    // Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (final byte[] key : keys) {
      int val = Integer.parseInt(new String(key, StandardCharsets.UTF_8));
      if (val % 3 == 0) {
        Assert.assertNull(binaryBTree.get(key));
      } else {
        Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    final NavigableSet<byte[]> keys =
        new TreeSet<>(OComparatorFactory.INSTANCE.getComparator(byte[].class));

    final int keysCount = 1_000_000;
    long seed = System.currentTimeMillis();
    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }
      byte[] key = Integer.toString(val).getBytes(StandardCharsets.UTF_8);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              binaryBTree.put(atomicOperation, key, new ORecordId(val % 32000, val)));
      keys.add(key);

      Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
    }

    Iterator<byte[]> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      byte[] key = keysIterator.next();

      if (Integer.parseInt(new String(key, StandardCharsets.UTF_8)) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> binaryBTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }
    //    Assert.assertEquals(binaryBTree.firstKey(), keys.first());
    //    Assert.assertEquals(binaryBTree.lastKey(), keys.last());

    for (final byte[] key : keys) {
      int val = Integer.parseInt(new String(key, StandardCharsets.UTF_8));
      if (val % 3 == 0) {
        Assert.assertNull(binaryBTree.get(key));
      } else {
        Assert.assertEquals(binaryBTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyAddDeleteHalf() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount / 2; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              binaryBTree.put(
                  atomicOperation,
                  Integer.toString(key).getBytes(StandardCharsets.UTF_8),
                  new ORecordId(key % 32000, key)));
    }

    for (int iterations = 0; iterations < 4; iterations++) {
      System.out.println("testKeyAddDeleteHalf : iteration " + iterations);

      for (int i = 0; i < keysCount / 2; i++) {
        final int key = i + (iterations + 1) * keysCount / 2;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                binaryBTree.put(
                    atomicOperation,
                    Integer.toString(key).getBytes(StandardCharsets.UTF_8),
                    new ORecordId(key % 32000, key)));

        Assert.assertEquals(
            binaryBTree.get(Integer.toString(key).getBytes(StandardCharsets.UTF_8)),
            new ORecordId(key % 32000, key));
      }

      final int offset = iterations * (keysCount / 2);

      for (int i = 0; i < keysCount / 2; i++) {
        final int key = i + offset;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    binaryBTree.remove(
                        atomicOperation, Integer.toString(key).getBytes(StandardCharsets.UTF_8)),
                    new ORecordId(key % 32000, key)));
      }

      final int start = (iterations + 1) * (keysCount / 2);
      for (int i = 0; i < (iterations + 2) * keysCount / 2; i++) {
        if (i < start) {
          Assert.assertNull(binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
        } else {
          Assert.assertEquals(
              new ORecordId(i % 32000, i),
              binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
        }
      }

      binaryBTree.assertFreePages();
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      final int k = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              binaryBTree.put(
                  atomicOperation,
                  Integer.toString(k).getBytes(StandardCharsets.UTF_8),
                  new ORecordId(k % 32000, k)));
    }

    final int txInterval = 100;
    for (int i = 0; i < keysCount / txInterval; i++) {
      final int iterationsCounter = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int j = 0; j < txInterval; j++) {
              final int key = iterationsCounter * txInterval + j;
              if (key % 3 == 0) {
                Assert.assertEquals(
                    binaryBTree.remove(
                        atomicOperation, Integer.toString(key).getBytes(StandardCharsets.UTF_8)),
                    new ORecordId(key % 32000, key));
              }
            }
          });
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
      } else {
        Assert.assertEquals(
            binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)),
            new ORecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              binaryBTree.put(
                  atomicOperation,
                  Integer.toString(key).getBytes(StandardCharsets.UTF_8),
                  new ORecordId(key % 32000, key)));

      Assert.assertEquals(
          binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)),
          new ORecordId(i % 32000, i));
    }
    final int txInterval = 100;

    for (int i = 0; i < keysCount / txInterval; i++) {
      final int iterationsCounter = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int j = 0; j < txInterval; j++) {
              final int key = (iterationsCounter * txInterval + j);

              if (key % 3 == 0) {
                Assert.assertEquals(
                    binaryBTree.remove(
                        atomicOperation, Integer.toString(key).getBytes(StandardCharsets.UTF_8)),
                    new ORecordId(key % 32000, key));
              }

              if (key % 2 == 0) {
                binaryBTree.put(
                    atomicOperation,
                    Integer.toString(keysCount + key).getBytes(StandardCharsets.UTF_8),
                    new ORecordId((keysCount + key) % 32000, keysCount + key));
              }
            }
          });
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
      } else {
        Assert.assertEquals(
            binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)),
            new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(
            binaryBTree.get(Integer.toString(keysCount + i).getBytes(StandardCharsets.UTF_8)),
            new ORecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testKeyAddDeleteAll() throws Exception {
    for (int iterations = 0; iterations < 4; iterations++) {
      System.out.println("testKeyAddDeleteAll : iteration " + iterations);

      final int keysCount = 1_000_000;

      for (int i = 0; i < keysCount; i++) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                binaryBTree.put(
                    atomicOperation,
                    Integer.toString(key).getBytes(StandardCharsets.UTF_8),
                    new ORecordId(key % 32000, key)));
      }

      for (int i = 0; i < keysCount; i++) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              Assert.assertEquals(
                  binaryBTree.remove(
                      atomicOperation, Integer.toString(key).getBytes(StandardCharsets.UTF_8)),
                  new ORecordId(key % 32000, key));

              if (key > 0 && key % 100_000 == 0) {
                for (int keyToVerify = 0; keyToVerify < keysCount; keyToVerify++) {
                  if (keyToVerify > key) {
                    Assert.assertEquals(
                        new ORecordId(keyToVerify % 32000, keyToVerify),
                        binaryBTree.get(
                            Integer.toString(keyToVerify).getBytes(StandardCharsets.UTF_8)));
                  } else {
                    Assert.assertNull(
                        binaryBTree.get(
                            Integer.toString(keyToVerify).getBytes(StandardCharsets.UTF_8)));
                  }
                }
              }
            });
      }
      for (int i = 0; i < keysCount; i++) {
        Assert.assertNull(binaryBTree.get(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
      }

      binaryBTree.assertFreePages();
    }
  }

  static final class RollbackException extends OException implements OHighLevelException {
    @SuppressWarnings("WeakerAccess")
    public RollbackException() {
      this("");
    }

    @SuppressWarnings("WeakerAccess")
    public RollbackException(String message) {
      super(message);
    }

    @SuppressWarnings("unused")
    public RollbackException(RollbackException exception) {
      super(exception);
    }
  }
}
