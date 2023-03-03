import { Action, ActionType } from '../actions';
import { makePosition, Entries, File } from '../types';

const DEFAULT_MAIN_RS = `fn main() {
    println!("Hello, world!");
}`;

const makeFile = (code: string): File => ({
  code,
  position: makePosition(0, 0),
  selection: {},
  lastEdited: new Date(),
});

const SINGLE_FILE_FILENAME = 'playground.rs';

export const makeState = (code: string): State => ({
  current: SINGLE_FILE_FILENAME,
  entries: { [SINGLE_FILE_FILENAME]: makeFile(code) },
});

const DEFAULT: State = makeState(DEFAULT_MAIN_RS);

export type State = {
  current: string;
  entries: Entries;
}

const reduceSingleFile = (state: State, callback: (entry: File) => File) => {
  let entries = state.entries;
  const entry = callback(entries[SINGLE_FILE_FILENAME]);
  entries = { ...entries, [SINGLE_FILE_FILENAME]: entry };
  return { ...state, entries };
};

const reduceSingleFileCode = (state: State, callback: (code: string) => string) => {
  return reduceSingleFile(state, (entry) => {
    const code = callback(entry.code);
    return { ...entry, code, lastEdited: new Date() };
  });
}

export default function files(state = DEFAULT, action: Action): State {
  switch (action.type) {
    // These are actions for the one-file mode

    case ActionType.RequestGistLoad:
      return reduceSingleFileCode(state, () => '');

    case ActionType.GistLoadSucceeded:
      return reduceSingleFileCode(state, () => action.code);

    case ActionType.EditCode:
      return reduceSingleFileCode(state, () => action.code);

    case ActionType.AddMainFunction:
      return reduceSingleFileCode(state, code => `${code}\n\n${DEFAULT_MAIN_RS}`);

    case ActionType.AddImport:
      return reduceSingleFileCode(state, code => action.code + code);

    case ActionType.EnableFeatureGate:
      return reduceSingleFileCode(state, code => `#![feature(${action.featureGate})]\n${code}`);

    case ActionType.FormatSucceeded:
      return reduceSingleFileCode(state, () => action.code);

    case ActionType.GotoPosition: {
      const { line, column } = action;
      return reduceSingleFile(state, entry => {
        return {...entry, position: { line, column }};
      });
    }

    case ActionType.SelectText: {
      const { start, end } = action;
      return reduceSingleFile(state, entry => {
        return {...entry, selection: { start, end }};
      });
    }

    default:
      return state;
  }
}
