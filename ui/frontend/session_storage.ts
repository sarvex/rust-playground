// This is used to store "short-term" values; those which we want to
// be preserved between the same sessions of the playground, such as
// when we reopen a closed tab.

import { State } from './reducers';
import {moveCode, removeVersion, initializeStorage, PartialState, MovedCode} from './storage';
import { codeSelector } from './selectors';
import { PrimaryAction } from './types';

const CURRENT_VERSION = 1;

interface V1Configuration {
  version: 1;
  configuration: {
    primaryAction: PrimaryAction,
  };
  code: string;
}

type CurrentConfiguration = V1Configuration;

export function serialize(state: State): string {
  const code = codeSelector(state);
  const conf: CurrentConfiguration = {
    version: CURRENT_VERSION,
    configuration: {
      primaryAction: state.configuration.primaryAction,
    },
    code,
  };
  return JSON.stringify(conf);
}

function rename(result: V1Configuration): MovedCode<V1Configuration> {
  return moveCode(result);
}

export function deserialize(savedState: string): PartialState {
  if (!savedState) { return undefined; }

  const parsedState = JSON.parse(savedState);
  if (!parsedState) { return undefined; }

  if (parsedState.version !== CURRENT_VERSION) { return undefined; }

  const renamed = rename(parsedState);

  return removeVersion(renamed);
}

export default initializeStorage({
  storageFactory: () => sessionStorage,
  serialize,
  deserialize,
});
