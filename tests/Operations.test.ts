import { describe, expect, it } from 'vitest';
import { createOperations } from '../src/Operations';
import { createCoordinate } from '@fjell/core';
import { Definition } from '../src/Definition';

describe('createOperations', () => {
  it('should create operations object with all methods', () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);
    
    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        useJsonExtension: true,
        keySharding: { enabled: false },
        querySafety: {}
      } as any
    };

    const operations = createOperations(mockStorage, definition);

    expect(operations).toBeDefined();
    expect(typeof operations).toBe('object');
    expect(typeof operations.get).toBe('function');
    expect(typeof operations.create).toBe('function');
    expect(typeof operations.update).toBe('function');
    expect(typeof operations.upsert).toBe('function');
    expect(typeof operations.remove).toBe('function');
    expect(typeof operations.all).toBe('function');
    expect(typeof operations.one).toBe('function');
  });

  it('should have finders, actions, and facets from options', () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);
    
    const testFinder = async () => [];
    const testAction = async (item: any) => [item, []];
    const testFacet = async () => ({ test: true });
    
    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        finders: { testFinder },
        actions: { testAction },
        facets: { testFacet }
      } as any
    };

    const operations = createOperations(mockStorage, definition);

    expect(operations.finders).toBeDefined();
    expect(operations.actions).toBeDefined();
    expect(operations.facets).toBeDefined();
    expect(operations.finders.testFinder).toBe(testFinder);
    expect(operations.actions.testAction).toBe(testAction);
    expect(operations.facets.testFacet).toBe(testFacet);
  });

  it('should have all extended operation methods', () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);
    
    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full'
      } as any
    };

    const operations = createOperations(mockStorage, definition);

    expect(typeof operations.find).toBe('function');
    expect(typeof operations.findOne).toBe('function');
    expect(typeof operations.action).toBe('function');
    expect(typeof operations.allAction).toBe('function');
    expect(typeof operations.facet).toBe('function');
    expect(typeof operations.allFacet).toBe('function');
  });

  it('should reject invalid finder return shape', async () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);

    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        finders: {
          badFinder: async () => ({ invalid: true })
        }
      } as any
    };

    const operations = createOperations(mockStorage, definition);

    await expect(
      operations.find('badFinder', {}, [])
    ).rejects.toThrow('returned invalid result');
  });

  it('should return finder result as-is when finder returns FindOperationResult', async () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);

    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        finders: {
          pagedFinder: async () => ({
            items: [{ kt: 'test', pk: '1', name: 'A' }],
            metadata: {
              total: 10,
              returned: 1,
              offset: 0,
              limit: 1,
              hasMore: true
            }
          })
        }
      } as any
    };

    const operations = createOperations(mockStorage, definition);
    const result = await operations.find('pagedFinder', {}, []);

    expect(result.items).toHaveLength(1);
    expect(result.metadata.total).toBe(10);
    expect(result.metadata.hasMore).toBe(true);
  });

  it('should treat undefined legacy finder result as empty array', async () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);

    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        finders: {
          emptyFinder: async () => undefined
        }
      } as any
    };

    const operations = createOperations(mockStorage, definition);
    const result = await operations.find('emptyFinder', {}, []);

    expect(result.items).toEqual([]);
    expect(result.metadata.total).toBe(0);
    expect(result.metadata.returned).toBe(0);
    expect(result.metadata.hasMore).toBe(false);
  });

  it('should throw when finder does not exist', async () => {
    const mockStorage = {} as any;
    const coordinate = createCoordinate(['test']);

    const definition: Definition<any, any> = {
      coordinate,
      bucketName: 'test-bucket',
      directoryPaths: ['tests'],
      basePath: '',
      options: {
        bucketName: 'test-bucket',
        mode: 'full',
        finders: {}
      } as any
    };

    const operations = createOperations(mockStorage, definition);

    await expect(
      operations.find('missingFinder', {}, [])
    ).rejects.toThrow('missingFinder');
  });
});
