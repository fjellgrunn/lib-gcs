import { beforeEach, describe, expect, it, vi } from 'vitest';
import { all } from '../../src/ops/all';
import { PathBuilder } from '../../src/PathBuilder';
import { FileProcessor } from '../../src/FileProcessor';
import { Item } from "@fjell/types";
import { createCoordinate } from "@fjell/core";

interface TestItem extends Item<'test'> {
  name: string;
  value: number;
}

describe('all operation', () => {
  let mockStorage: any;
  let mockBucket: any;
  let pathBuilder: PathBuilder;
  let fileProcessor: FileProcessor;
  const bucketName = 'test-bucket';
  const coordinate = createCoordinate(['test']);

  beforeEach(() => {
    mockBucket = {
      getFiles: vi.fn(),
    };

    mockStorage = {
      bucket: vi.fn().mockReturnValue(mockBucket),
    };

    pathBuilder = new PathBuilder({
      bucketName,
      directoryPaths: ['tests'],
      useJsonExtension: true,
    });

    fileProcessor = new FileProcessor();
  });

  it('should get all items from bucket', async () => {
    const item1: TestItem = { kt: 'test', pk: 'test-1', name: 'Item 1', value: 1 };
    const item2: TestItem = { kt: 'test', pk: 'test-2', name: 'Item 2', value: 2 };

    const mockFiles = [
      {
        name: 'test/test-1.json',
        download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item1))])
      },
      {
        name: 'test/test-2.json',
        download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item2))])
      }
    ];

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      undefined,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(2);
    expect(result.items[0].name).toBe('Item 1');
    expect(result.items[1].name).toBe('Item 2');
    expect(result.metadata.total).toBe(2);
  });

  it('should apply query limit', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'Item 1', value: 1 },
      { kt: 'test', pk: '2', name: 'Item 2', value: 2 },
      { kt: 'test', pk: '3', name: 'Item 3', value: 3 }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { limit: 2 },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(2);
    expect(result.metadata.total).toBe(3);
  });

  it('should apply query offset', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'Item 1', value: 1 },
      { kt: 'test', pk: '2', name: 'Item 2', value: 2 },
      { kt: 'test', pk: '3', name: 'Item 3', value: 3 }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { offset: 1 },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(2);
    expect(result.items[0].name).toBe('Item 2');
    expect(result.metadata.total).toBe(3);
  });

  it('should reject negative limit values', async () => {
    mockBucket.getFiles.mockResolvedValue([[]]);

    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        { limit: -1 },
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        { bucketName, mode: 'full' } as any
      )
    ).rejects.toThrow('Invalid pagination limit for all operation');
  });

  it('should reject negative offset values', async () => {
    mockBucket.getFiles.mockResolvedValue([[]]);

    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        { offset: -1 },
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        { bucketName, mode: 'full' } as any
      )
    ).rejects.toThrow('Invalid pagination offset for all operation');
  });

  it('should reject non-integer pagination values', async () => {
    mockBucket.getFiles.mockResolvedValue([[]]);

    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        { limit: 1.5 as any },
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        { bucketName, mode: 'full' } as any
      )
    ).rejects.toThrow('Invalid pagination limit for all operation');
  });

  it('should throw error if file count exceeds maxScanFiles', async () => {
    const mockFiles = Array.from({ length: 1500 }, (_, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn()
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        undefined,
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        {
          bucketName,
          mode: 'full',
          querySafety: { maxScanFiles: 1000 }
        } as any
      )
    ).rejects.toThrow('File count (1500) exceeds maxScanFiles limit (1000)');
  });

  it('should throw error if query operations are disabled', async () => {
    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        undefined,
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        {
          bucketName,
          mode: 'full',
          querySafety: { disableQueryOperations: true }
        } as any
      )
    ).rejects.toThrow('Query operations are disabled');
  });

  it('should throw error in files-only mode', async () => {
    await expect(
      all<TestItem, 'test'>(
        mockStorage,
        bucketName,
        undefined,
        undefined,
        pathBuilder,
        fileProcessor,
        coordinate,
        { bucketName, mode: 'files-only' } as any
      )
    ).rejects.toThrow('Item operations are disabled in files-only mode');
  });

  it('should filter non-JSON files', async () => {
    const item: TestItem = { kt: 'test', pk: 'test-1', name: 'Item 1', value: 1 };

    const mockFiles = [
      {
        name: 'test/test-1.json',
        download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
      },
      {
        name: 'test/test-2.txt', // Not JSON
        download: vi.fn()
      }
    ];

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      undefined,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(1);
    expect(result.metadata.total).toBe(1);
    expect(mockFiles[1].download).not.toHaveBeenCalled(); // TXT file not downloaded
  });

  it('should handle download errors gracefully', async () => {
    const item1: TestItem = { kt: 'test', pk: 'test-1', name: 'Item 1', value: 1 };

    const mockFiles = [
      {
        name: 'test/test-1.json',
        download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item1))])
      },
      {
        name: 'test/test-2.json',
        download: vi.fn().mockRejectedValue(new Error('Download failed'))
      }
    ];

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      undefined,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(1); // Only successful download
    expect(result.items[0].name).toBe('Item 1');
    expect(result.metadata.total).toBe(1);
  });
});

