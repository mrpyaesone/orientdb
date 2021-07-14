package com.orientechnologies.orient.core.storage.fs;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncFile implements OFile {
  private final ScalableRWLock lock = new ScalableRWLock();
  private volatile Path osFile;

  private final AtomicLong dirtyCounter = new AtomicLong();
  private final Object flushSemaphore = new Object();

  private final AtomicLong size = new AtomicLong(-1);
  private FileChannel writeChannel;
  // unlike writes reads can be safely interrupted by Thread.interrupt so those two channels are
  // separated.
  private FileChannel readChannel;

  private final int pageSize;

  public AsyncFile(final Path osFile, final int pageSize) {
    this.osFile = osFile;
    this.pageSize = pageSize;
  }

  @Override
  public void create() throws IOException {
    lock.exclusiveLock();
    try {
      if (writeChannel != null || readChannel != null) {
        throw new OStorageException("File " + osFile + " is already opened.");
      }

      Files.createFile(osFile);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void initSize() throws IOException {
    if (writeChannel.size() < HEADER_SIZE) {
      final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

      OIOUtils.writeByteBuffer(buffer, writeChannel, 0);
      dirtyCounter.incrementAndGet();
    }

    long currentSize = writeChannel.size() - HEADER_SIZE;

    if (currentSize % pageSize != 0) {
      final long initialSize = currentSize;

      currentSize = (currentSize / pageSize) * pageSize;
      writeChannel.truncate(currentSize + HEADER_SIZE);

      OLogManager.instance()
          .warnNoDb(
              this,
              "Data page in file {} was partially written and will be truncated, "
                  + "initial size {}, truncated size {}",
              osFile,
              initialSize,
              currentSize);
    }

    if (size.get() < 0) {
      size.set(currentSize);
    } else {
      if (writeChannel.size() - HEADER_SIZE > size.get()) {
        throw new IllegalStateException(
            "Physical size of the file "
                + (writeChannel.size() - HEADER_SIZE)
                + " but logical size is "
                + size.get());
      }
    }
  }

  @Override
  public void open() {
    lock.exclusiveLock();
    try {
      doOpen();
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Can not open file " + osFile), e);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void doOpen() throws IOException {
    if (writeChannel != null || readChannel != null) {
      throw new OStorageException("File " + osFile + " is already opened.");
    }

    writeChannel = FileChannel.open(osFile, StandardOpenOption.WRITE);
    readChannel = FileChannel.open(osFile, StandardOpenOption.READ);

    initSize();
  }

  @Override
  public long getFileSize() {
    return size.get();
  }

  @Override
  public String getName() {
    return osFile.getFileName().toString();
  }

  @Override
  public boolean isOpen() {
    lock.sharedLock();
    try {
      return writeChannel != null;
    } finally {
      lock.sharedUnlock();
    }
  }

  @Override
  public boolean exists() {
    return Files.exists(osFile);
  }

  @Override
  public void write(long offset, ByteBuffer buffer) {
    lock.sharedLock();
    try {
      buffer.rewind();

      checkForClose();
      checkPosition(offset);
      checkPosition(offset + buffer.limit() - 1);

      try {
        OIOUtils.writeByteBuffer(buffer, writeChannel, offset + HEADER_SIZE);
      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Error during writing of file " + osFile), e);
      }

      dirtyCounter.incrementAndGet();
    } finally {
      lock.sharedUnlock();
    }
  }

  @Override
  public IOResult write(List<ORawPair<Long, ByteBuffer>> buffers) {

    for (final ORawPair<Long, ByteBuffer> pair : buffers) {
      final ByteBuffer byteBuffer = pair.second;
      byteBuffer.rewind();
      lock.sharedLock();
      try {
        checkForClose();
        checkPosition(pair.first);
        checkPosition(pair.first + pair.second.limit() - 1);

        final long position = pair.first + HEADER_SIZE;
        try {
          OIOUtils.writeByteBuffer(byteBuffer, writeChannel, position);
        } catch (IOException e) {
          throw OException.wrapException(
              new OStorageException("Error during writing of file " + osFile), e);
        }
      } finally {
        lock.sharedUnlock();
      }
    }

    return () -> {};
  }

  @Override
  public void read(long offset, ByteBuffer buffer, boolean throwOnEof) throws IOException {
    try {
      lock.sharedLock();
      try {
        checkForClose();
        checkPosition(offset);

        OIOUtils.readByteBuffer(buffer, readChannel, offset + HEADER_SIZE, true);
      } finally {
        lock.sharedUnlock();
      }
    } catch (final ClosedByInterruptException cbe) {
      lock.exclusiveLock();
      try {
        readChannel = FileChannel.open(osFile, StandardOpenOption.READ);
        throw OException.wrapException(
            new OInterruptedException("Read of file " + osFile + " was interrupted."), cbe);
      } finally {
        lock.exclusiveUnlock();
      }
    }
  }

  @Override
  public long allocateSpace(int size) {
    return this.size.getAndAdd(size);
  }

  @Override
  public void shrink(long size) throws IOException {
    lock.exclusiveLock();
    try {
      checkForClose();

      this.size.set(0);
      writeChannel.truncate(size + HEADER_SIZE);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void synch() {
    lock.sharedLock();
    try {
      doSynch();
    } finally {
      lock.sharedUnlock();
    }
  }

  private void doSynch() {
    synchronized (flushSemaphore) {
      long dirtyCounterValue = dirtyCounter.get();
      if (dirtyCounterValue > 0) {
        try {
          writeChannel.force(false);
        } catch (final IOException e) {
          OLogManager.instance()
              .warn(
                  this,
                  "Error during flush of file %s. Data may be lost in case of power failure",
                  e,
                  getName());
        }

        dirtyCounter.addAndGet(-dirtyCounterValue);
      }
    }
  }

  @Override
  public void close() {
    lock.exclusiveLock();
    try {
      doSynch();
      doClose();
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during closing the file " + osFile), e);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void doClose() throws IOException {
    // ignore if closed
    if (writeChannel != null) {
      writeChannel.close();
      writeChannel = null;
    }
    if (readChannel != null) {
      readChannel.close();
      readChannel = null;
    }
  }

  @Override
  public void delete() throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      Files.delete(osFile);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void renameTo(Path newFile) throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      //noinspection NonAtomicOperationOnVolatileField
      osFile = Files.move(osFile, newFile);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void replaceContentWith(final Path newContentFile) throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      Files.copy(newContentFile, osFile, StandardCopyOption.REPLACE_EXISTING);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void checkPosition(long offset) {
    final long fileSize = size.get();
    if (offset < 0 || offset >= fileSize) {
      throw new OStorageException(
          "You are going to access region outside of allocated file position. File size = "
              + fileSize
              + ", requested position "
              + offset);
    }
  }

  private void checkForClose() {
    if (writeChannel == null || readChannel == null) {
      throw new OStorageException("File " + osFile + " is closed");
    }
  }
}
