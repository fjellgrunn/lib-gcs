import { Storage } from '@google-cloud/storage';
import { AllOperationResult, AllOptions, CompoundCondition, Condition, Coordinate, Item, ItemQuery, LocKeyArray, OrderBy } from '@fjell/types';
import { PathBuilder } from '../PathBuilder';
import { FileProcessor } from '../FileProcessor';
import { Options } from '../Options';
import GCSLogger from '../logger';

const logger = GCSLogger.get('ops', 'all');

const isNonNegativeInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value >= 0;

const validatePaginationValue = (value: unknown, name: 'limit' | 'offset'): void => {
  if (typeof value === 'undefined' || value === null) {
    return;
  }

  if (!isNonNegativeInteger(value)) {
    throw new Error(
      `Invalid pagination ${name} for all operation. ` +
      `Expected a non-negative integer, received: ${String(value)}`
    );
  }
};

const compareValues = (a: unknown, b: unknown): number => {
  // Keep null/undefined values at the end for deterministic ordering.
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }

  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};

const valuesEqual = (a: unknown, b: unknown): boolean => {
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  return a === b;
};

const evaluateCondition = (item: any, condition: Condition): boolean => {
  const itemValue = (item as any)[condition.column];
  const conditionValue = condition.value;

  switch (condition.operator) {
    case '==':
      return valuesEqual(itemValue, conditionValue);
    case '!=':
      return !valuesEqual(itemValue, conditionValue);
    case '>':
      return itemValue > conditionValue;
    case '>=':
      return itemValue >= conditionValue;
    case '<':
      return itemValue < conditionValue;
    case '<=':
      return itemValue <= conditionValue;
    case 'in':
      return Array.isArray(conditionValue) && conditionValue.some(value => valuesEqual(itemValue, value));
    case 'not-in':
      return Array.isArray(conditionValue) && !conditionValue.some(value => valuesEqual(itemValue, value));
    case 'array-contains':
      return Array.isArray(itemValue) && itemValue.some(value => valuesEqual(value, conditionValue));
    case 'array-contains-any':
      return Array.isArray(itemValue) &&
        Array.isArray(conditionValue) &&
        conditionValue.some(value => itemValue.some(itemElement => valuesEqual(itemElement, value)));
    default:
      return false;
  }
};

const evaluateCompoundCondition = (item: any, compound: CompoundCondition): boolean => {
  const matches = compound.conditions.map((condition) => {
    if ('compoundType' in condition) {
      return evaluateCompoundCondition(item, condition);
    }

    return evaluateCondition(item, condition);
  });

  if (compound.compoundType === 'OR') {
    return matches.some(Boolean);
  }

  return matches.every(Boolean);
};

/**
 * Get all items matching a query from GCS with pagination support
 * ⚠️ WARNING: Downloads and filters in-memory. Not suitable for large datasets.
 */
export async function all<
  V extends Item<S, L1, L2, L3, L4, L5>,
  S extends string,
  L1 extends string = never,
  L2 extends string = never,
  L3 extends string = never,
  L4 extends string = never,
  L5 extends string = never
>(
  storage: Storage,
  bucketName: string,
  query: ItemQuery | undefined,
  locations: LocKeyArray<L1, L2, L3, L4, L5> | [] | undefined,
  pathBuilder: PathBuilder,
  fileProcessor: FileProcessor,
  coordinate: Coordinate<S, L1, L2, L3, L4, L5>,
  options: Options<V, S, L1, L2, L3, L4, L5>,
  allOptions?: AllOptions
): Promise<AllOperationResult<V>> {
  logger.default('all', { query, locations, bucketName, allOptions });

  // Check if in files-only mode
  if (options.mode === 'files-only') {
    throw new Error(
      `Item operations are disabled in files-only mode. ` +
      `This library is configured to handle only file attachments. ` +
      `Use the primary library (e.g., lib-firestore) for item operations.`
    );
  }

  // Check if query operations are disabled
  if (options.querySafety?.disableQueryOperations) {
    throw new Error(
      `Query operations are disabled via querySafety.disableQueryOperations. ` +
      `This prevents expensive in-memory filtering on large datasets. ` +
      `Use get() with exact keys or enable query operations in Options.`
    );
  }

  try {
    const bucket = storage.bucket(bucketName);
    
    // Determine directory prefix based on locations
    let prefix: string;
    
    if (locations && locations.length > 0) {
      // Build prefix from locations
      const locationPath = pathBuilder.buildDirectoryFromLocations(locations as any[]);
      const kt = coordinate.kta[0];
      prefix = locationPath ? `${locationPath}/${kt}` : kt;
    } else {
      // No locations specified - list all items of this type
      const kt = coordinate.kta[0];
      prefix = pathBuilder.getBasePath();
      if (prefix) {
        prefix += '/';
      }
      prefix += kt;
    }

    logger.default('Listing files', { prefix, locations });

    // List all files in directory
    const [files] = await bucket.getFiles({
      prefix: prefix
    });

    // Filter to only JSON files
    const jsonFiles = files.filter(file =>
      options.useJsonExtension !== false ? file.name.endsWith('.json') : true
    );

    logger.default('Found files', { count: jsonFiles.length });

    // Apply query safety checks
    const maxScanFiles = options.querySafety?.maxScanFiles || 1000;
    const warnThreshold = options.querySafety?.warnThreshold || 100;

    if (jsonFiles.length > maxScanFiles) {
      throw new Error(
        `File count (${jsonFiles.length}) exceeds maxScanFiles limit (${maxScanFiles}). ` +
        `Consider: 1) Increasing the limit, 2) Using get() with exact keys, ` +
        `3) Using @fjell/lib-firestore for queryable data, or 4) Implementing an external index.`
      );
    }

    if (jsonFiles.length > warnThreshold) {
      logger.default('WARNING: File count exceeds warning threshold', {
        count: jsonFiles.length,
        threshold: warnThreshold
      });
    }

    // Download files with concurrency control
    const downloadConcurrency = options.querySafety?.downloadConcurrency || 10;
    const items: V[] = [];

    for (let i = 0; i < jsonFiles.length; i += downloadConcurrency) {
      const batch = jsonFiles.slice(i, i + downloadConcurrency);
      
      const batchResults = await Promise.all(
        batch.map(async (file) => {
          try {
            const [content] = await file.download();
            const item = fileProcessor.deserializeFromBuffer<V>(content, coordinate as any);
            return item;
          } catch (error) {
            logger.error('Error downloading/deserializing file', { file: file.name, error });
            return null;
          }
        })
      );

      items.push(...batchResults.filter((item): item is NonNullable<typeof item> => item !== null) as V[]);
    }

    logger.default('Downloaded and deserialized items', { count: items.length });

    // Apply query filters if provided (but NOT pagination yet)
    let filtered = items;

    // Canonical ItemQuery shape
    if (query?.compoundCondition) {
      filtered = filtered.filter(item => evaluateCompoundCondition(item, query.compoundCondition as CompoundCondition));
    }
    // Backward compatibility for legacy query shape
    else if (query && (query as any).filter) {
      filtered = filtered.filter(item => {
        for (const [key, value] of Object.entries((query as any).filter)) {
          if ((item as any)[key] !== value) {
            return false;
          }
        }
        return true;
      });
    }

    // Apply sorting if provided
    const orderBy: Array<OrderBy | { field: string; direction?: 'asc' | 'desc' }> | undefined =
      query?.orderBy ?? (query as any)?.sort;
    if (orderBy && orderBy.length > 0) {
      filtered.sort((a, b) => {
        for (const { field, direction } of orderBy) {
          const aVal = (a as any)[field];
          const bVal = (b as any)[field];
          const comparison = compareValues(aVal, bVal);

          if (comparison !== 0) {
            return direction === 'desc' ? -comparison : comparison;
          }
        }
        return 0;
      });
    }

    // Get total count BEFORE applying pagination
    const total = filtered.length;

    // Determine effective limit/offset (allOptions takes precedence over query)
    const effectiveLimit = allOptions?.limit ?? query?.limit;
    const effectiveOffset = allOptions?.offset ?? query?.offset ?? 0;

    validatePaginationValue(effectiveLimit, 'limit');
    validatePaginationValue(effectiveOffset, 'offset');

    logger.default('Pagination', { total, effectiveLimit, effectiveOffset });

    // Apply pagination
    let result = filtered;

    // Apply offset
    if (effectiveOffset > 0) {
      result = result.slice(effectiveOffset);
    }

    // Apply limit
    if (effectiveLimit != null) {
      result = result.slice(0, effectiveLimit);
    }

    logger.default('Filtered, sorted and paginated items', { count: result.length, total });

    // Return AllOperationResult with items and metadata
    return {
      items: result,
      metadata: {
        total,
        returned: result.length,
        limit: effectiveLimit,
        offset: effectiveOffset,
        hasMore: effectiveOffset + result.length < total
      }
    };
  } catch (error) {
    logger.error('Error getting all items', { error });
    throw error;
  }
}

