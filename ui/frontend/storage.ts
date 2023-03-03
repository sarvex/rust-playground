import { DeepPartial } from 'ts-essentials';

import State from './state';
import { makeState } from './reducers/files';

type SimpleStorage = Pick<Storage, 'getItem' | 'setItem'>;

export type PartialState = DeepPartial<State> | undefined;

type StorageFactory = () => SimpleStorage;

interface Config {
  storageFactory: StorageFactory;
  serialize: (state: State) => string;
  deserialize: (state: string) => PartialState;
}

interface InitializedStorage {
  initialState: PartialState;
  saveChanges: (state: State) => void;
}

type FileStateSlice = Pick<State, 'files'>;
export type MovedCode<T> = Omit<T, 'code'> & FileStateSlice;

export function moveCode<T extends { code: string }>(data: T): MovedCode<T> {
  const code = data.code;

  const munged: Record<string, unknown> = {...data};
  delete munged.code;
  const removed = munged as Omit<T, 'code'>;

  return { ...removed, files: makeState(code) };
}

export function removeVersion<T extends { version: unknown }>(data: T): Omit<T, 'version'> {
  const munged: Record<string, unknown> = {...data};
  delete munged.version;
  return munged as Omit<T, 'version'>
}

export class InMemoryStorage {
  private data: { [s: string]: string } = {};

  public getItem(name: string): string {
    return this.data[name];
  }

  public setItem(name: string, value: string) {
    this.data[name] = value;
  }
}

const KEY = 'redux';

export function initializeStorage(config: Config) {
  return (): InitializedStorage => {
    const { storageFactory, serialize, deserialize } = config;

    const storage = validateStorage(storageFactory);
    const serializedState = storage.getItem(KEY);
    const initialState = serializedState ? deserialize(serializedState) : undefined;

    const saveChanges = (state: State) => {
      const serializedState = serialize(state);
      storage.setItem(KEY, serializedState);
    };

    return { initialState, saveChanges }
  }
}

// Attempt to use the storage to see if security settings are
// preventing it. Falls back to dummy in-memory storage if needed.
function validateStorage(storageFactory: StorageFactory): SimpleStorage {
  try {
    const storage = storageFactory();
    const current = storage.getItem(KEY);
    storage.setItem(KEY, current || '');
    return storage;

  } catch (e) {
    console.warn('Unable to store configuration, falling back to non-persistent in-memory storage');
    return new InMemoryStorage();
  }
}
