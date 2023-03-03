import { Action, ActionType } from '../actions';
import { makePosition, Entries, File } from '../types';

const DEFAULT_MAIN_RS = `fn main() {
    println!("Hello, world!");
}`;

const DEFAULT_CARGO_TOML = `[package]
name = "playground"
version = "0.0.1"
authors = ["The Rust Playground"]
resolver = "2"

[profile.dev]
codegen-units = 1
incremental = false

[profile.dev.build-override]
codegen-units = 1

[profile.release]
codegen-units = 1
incremental = false

[profile.release.build-override]
codegen-units = 1

[dependencies.itertools]
package = "itertools"
version = "=0.10.5"
features = ["use_alloc", "use_std"]
`

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

// const DEFAULT: State = {
//   current: 'main.rs',
//   entries: {
//     'main.rs': makeFile(DEFAULT_MAIN_RS),
//     'Cargo.toml': makeFile(DEFAULT_CARGO_TOML),
//   },
// };

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

    // These are actions for the multi-file mode

    case ActionType.EditFile: {
      const { name, code } = action;
      let entries = state.entries;
      let entry = entries[name];
      entry = { ...entry, code, lastEdited: new Date() };
      entries = { ...entries, [name]: entry };
      return { ...state, entries };
    }

    case ActionType.SelectFile: {
      return { ...state, current: action.name };
    }

    case ActionType.SaveFiles: {
      const { entries: toSave } = action;
      let entries = state.entries;
      const lastSaved = new Date();
      for (const name of Object.keys(toSave)) {
        let entry = entries[name];
        entry = { ...entry, lastSaved };
        entries = { ...entries, [name]: entry };
      }
      return { ...state, entries };
    }

    default:
      return state;
  }
}
