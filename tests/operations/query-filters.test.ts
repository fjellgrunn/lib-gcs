import { beforeEach, describe, expect, it, vi } from 'vitest';
import { all } from '../../src/ops/all';
import { PathBuilder } from '../../src/PathBuilder';
import { FileProcessor } from '../../src/FileProcessor';
import { Item } from "@fjell/types";
import { createCoordinate } from "@fjell/core";

interface TestItem extends Item<'test'> {
  name: string;
  value: number;
  category?: string;
  tags?: string[];
  status?: string;
}

describe('Query Filters and Sorting', () => {
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

  it('should apply filter query', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'Item 1', value: 1, category: 'A' },
      { kt: 'test', pk: '2', name: 'Item 2', value: 2, category: 'B' },
      { kt: 'test', pk: '3', name: 'Item 3', value: 3, category: 'A' }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { filter: { category: 'A' } } as any,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(2);
    expect(result.items[0].category).toBe('A');
    expect(result.items[1].category).toBe('A');
    expect(result.metadata.total).toBe(2);
  });

  it('should apply sort ascending', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'C', value: 3 },
      { kt: 'test', pk: '2', name: 'A', value: 1 },
      { kt: 'test', pk: '3', name: 'B', value: 2 }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { sort: [{ field: 'name', direction: 'asc' }] } as any,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items[0].name).toBe('A');
    expect(result.items[1].name).toBe('B');
    expect(result.items[2].name).toBe('C');
    expect(result.metadata.total).toBe(3);
  });

  it('should apply sort descending', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1 },
      { kt: 'test', pk: '2', name: 'B', value: 2 },
      { kt: 'test', pk: '3', name: 'C', value: 3 }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { sort: [{ field: 'value', direction: 'desc' }] } as any,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items[0].value).toBe(3);
    expect(result.items[1].value).toBe(2);
    expect(result.items[2].value).toBe(1);
    expect(result.metadata.total).toBe(3);
  });

  it('should combine filter, sort, offset, and limit', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1, category: 'X' },
      { kt: 'test', pk: '2', name: 'B', value: 2, category: 'X' },
      { kt: 'test', pk: '3', name: 'C', value: 3, category: 'X' },
      { kt: 'test', pk: '4', name: 'D', value: 4, category: 'Y' },
      { kt: 'test', pk: '5', name: 'E', value: 5, category: 'X' }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      {
        filter: { category: 'X' },
        sort: [{ field: 'value', direction: 'desc' }],
        offset: 1,
        limit: 2
      } as any,
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    // Filter: category='X' -> [A,B,C,E]
    // Sort desc by value: [E(5),C(3),B(2),A(1)]
    // Offset 1: [C(3),B(2),A(1)]
    // Limit 2: [C(3),B(2)]
    expect(result.items).toHaveLength(2);
    expect(result.items[0].name).toBe('C');
    expect(result.items[1].name).toBe('B');
    expect(result.metadata.total).toBe(4); // Total matching filter
  });

  it('should apply canonical ItemQuery compoundCondition and orderBy', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1, category: 'X' },
      { kt: 'test', pk: '2', name: 'B', value: 2, category: 'X' },
      { kt: 'test', pk: '3', name: 'C', value: 3, category: 'Y' },
      { kt: 'test', pk: '4', name: 'D', value: 4, category: 'X' }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      {
        compoundCondition: {
          compoundType: 'AND',
          conditions: [
            { column: 'category', operator: '==', value: 'X' },
            {
              compoundType: 'OR',
              conditions: [
                { column: 'value', operator: '>=', value: 4 },
                { column: 'value', operator: '<=', value: 1 }
              ]
            }
          ]
        },
        orderBy: [{ field: 'value', direction: 'desc' }]
      },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    // category X with value <=1 OR >=4 => D(4), A(1); ordered desc by value
    expect(result.items).toHaveLength(2);
    expect(result.items[0].name).toBe('D');
    expect(result.items[1].name).toBe('A');
    expect(result.metadata.total).toBe(2);
  });

  it('should support in and array-contains-any compound operators', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1, status: 'active', tags: ['news', 'tech'] },
      { kt: 'test', pk: '2', name: 'B', value: 2, status: 'draft', tags: ['lifestyle'] },
      { kt: 'test', pk: '3', name: 'C', value: 3, status: 'archived', tags: ['tech'] }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      {
        compoundCondition: {
          compoundType: 'AND',
          conditions: [
            { column: 'status', operator: 'in', value: ['active', 'archived'] },
            { column: 'tags', operator: 'array-contains-any', value: ['tech', 'finance'] }
          ]
        }
      },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items).toHaveLength(2);
    expect(result.items.map(i => i.name).sort()).toEqual(['A', 'C']);
    expect(result.metadata.total).toBe(2);
  });

  it('should support not-in operator', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1, status: 'active' },
      { kt: 'test', pk: '2', name: 'B', value: 2, status: 'draft' },
      { kt: 'test', pk: '3', name: 'C', value: 3, status: 'archived' }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      {
        compoundCondition: {
          compoundType: 'AND',
          conditions: [
            { column: 'status', operator: 'not-in', value: ['draft'] }
          ]
        },
        orderBy: [{ field: 'name', direction: 'asc' }]
      },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items.map(i => i.name)).toEqual(['A', 'C']);
    expect(result.metadata.total).toBe(2);
  });

  it('should place null and undefined sort values at the end', async () => {
    const items = [
      { kt: 'test', pk: '1', name: 'A', value: 1, category: 'X' },
      { kt: 'test', pk: '2', name: 'B', value: 2 },
      { kt: 'test', pk: '3', name: 'C', value: 3, category: 'A' }
    ];

    const mockFiles = items.map((item, i) => ({
      name: `test/test-${i}.json`,
      download: vi.fn().mockResolvedValue([Buffer.from(JSON.stringify(item))])
    }));

    mockBucket.getFiles.mockResolvedValue([mockFiles]);

    const result = await all<TestItem, 'test'>(
      mockStorage,
      bucketName,
      { orderBy: [{ field: 'category', direction: 'asc' }] },
      undefined,
      pathBuilder,
      fileProcessor,
      coordinate,
      { bucketName, mode: 'full' } as any
    );

    expect(result.items[0].name).toBe('C');
    expect(result.items[1].name).toBe('A');
    expect(result.items[2].name).toBe('B');
  });
});

