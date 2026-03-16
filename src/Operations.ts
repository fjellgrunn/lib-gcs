import { AllOptions, ComKey, FindOperationResult, Item, ItemQuery, LocKeyArray, PriKey } from "@fjell/types";
import { Storage } from '@google-cloud/storage';
import * as Library from '@fjell/lib';
import { Definition } from './Definition';
import { PathBuilder } from './PathBuilder';
import { FileProcessor } from './FileProcessor';
import * as ops from './ops';
import GCSLogger from './logger';
import { Registry } from '@fjell/lib';

const logger = GCSLogger.get('Operations');

/**
 * Create Operations implementation for GCS Library
 */
export const createOperations = <
  V extends Item<S, L1, L2, L3, L4, L5>,
  S extends string,
  L1 extends string = never,
  L2 extends string = never,
  L3 extends string = never,
  L4 extends string = never,
  L5 extends string = never
>(
    storage: Storage,
    definition: Definition<V, S, L1, L2, L3, L4, L5>,
   
    _registry?: Registry
  ): Library.Operations<V, S, L1, L2, L3, L4, L5> => {
  logger.default('createOperations', {
    bucketName: definition.bucketName,
    coordinate: definition.coordinate
  });

  // Create PathBuilder and FileProcessor instances
  const pathBuilder = new PathBuilder({
    bucketName: definition.bucketName,
    directoryPaths: definition.directoryPaths,
    basePath: definition.basePath,
    useJsonExtension: definition.options.useJsonExtension,
    keySharding: definition.options.keySharding
  });

  const fileProcessor = new FileProcessor();

  const { bucketName, options, coordinate } = definition;

  // Create implementation operations (core CRUD and query operations)
  const implOps: Library.ImplementationOperations<V, S, L1, L2, L3, L4, L5> = {
    get: async (key: PriKey<S> | ComKey<S, L1, L2, L3, L4, L5>) => {
      return ops.get<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        key,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    create: async (
      item: Partial<Item<S, L1, L2, L3, L4, L5>>,
      createOptions?: ops.CreateOptions<S, L1, L2, L3, L4, L5>
    ) => {
      return ops.create<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        item,
        createOptions,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    update: async (
      key: PriKey<S> | ComKey<S, L1, L2, L3, L4, L5>,
      item: Partial<Item<S, L1, L2, L3, L4, L5>>,
      updateOptions?: ops.UpdateOptions
    ) => {
      return ops.update<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        key,
        item,
        updateOptions,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    upsert: async (
      key: PriKey<S> | ComKey<S, L1, L2, L3, L4, L5>,
      item: Partial<Item<S, L1, L2, L3, L4, L5>>,
      locations?: LocKeyArray<L1, L2, L3, L4, L5>,
      updateOptions?: ops.UpdateOptions
    ) => {
      return ops.upsert<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        key,
        item,
        locations,
        updateOptions,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    remove: async (key: PriKey<S> | ComKey<S, L1, L2, L3, L4, L5>) => {
      return ops.remove<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        key,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    all: async (
      query?: ItemQuery,
      locations?: LocKeyArray<L1, L2, L3, L4, L5> | [],
      allOptions?: AllOptions
    ) => {
      return ops.all<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        query,
        locations,
        pathBuilder,
        fileProcessor,
        coordinate,
        options,
        allOptions
      );
    },

    one: async (
      query?: ItemQuery,
      locations?: LocKeyArray<L1, L2, L3, L4, L5> | []
    ) => {
      return ops.one<V, S, L1, L2, L3, L4, L5>(
        storage,
        bucketName,
        query,
        locations,
        pathBuilder,
        fileProcessor,
        coordinate,
        options
      );
    },

    // Find operations - execute user-defined finders
    find: async (finder: string, params: any = {}, locations?: any, findOptions?: any): Promise<FindOperationResult<V>> => {
      if (!options.finders || !options.finders[finder]) {
        throw new Error(`Finder '${finder}' not found`);
      }
      
      // Execute user's finder function - pass findOptions for opt-in pagination support
      // Finder can return FindOperationResult<V> (opt-in) or V[] (legacy)
      // Type assertion needed because FinderMethod type from @fjell/lib may be stale
      const finderResult = await (options.finders[finder] as any)(params, locations, findOptions);
      
      // Check if finder opted-in (returned FindOperationResult) or legacy (returned V[])
      if (finderResult && typeof finderResult === 'object' && 'items' in finderResult && 'metadata' in finderResult) {
        // Finder opted-in: return as-is
        return finderResult as FindOperationResult<V>;
      } else {
        // Legacy finder: return value must be an array
        if (typeof finderResult !== 'undefined' && finderResult !== null && !Array.isArray(finderResult)) {
          throw new Error(
            `Finder '${finder}' returned invalid result. ` +
            `Expected an array (legacy mode) or FindOperationResult with { items, metadata }.`
          );
        }

        const items = (finderResult ?? []) as V[];
        return {
          items,
          metadata: {
            total: items.length,
            returned: items.length,
            offset: 0,
            hasMore: false
          }
        };
      }
    },

    findOne: async (finder: string, params: any = {}, locations?: any) => {
      if (!options.finders || !options.finders[finder]) {
        throw new Error(`Finder '${finder}' not found`);
      }
      
      // Call finder with limit: 1 and extract first item
      // Type assertion needed because FinderMethod type from @fjell/lib may be stale
      const result = await (options.finders[finder] as any)(params, locations, { limit: 1 });
      // Handle both FindOperationResult and V[] return types
      if (result && typeof result === 'object' && 'items' in result && 'metadata' in result) {
        return result.items.length > 0 ? result.items[0] : null;
      }
      const results = result as V[];
      return results && results.length > 0 ? results[0] : null;
    }
  };

  // Wrap with hooks, validation, and extended operations using @fjell/lib wrapper
  return Library.wrapOperations(implOps as any, options, coordinate, _registry || ({} as Registry));
};
