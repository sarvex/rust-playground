import React, { useCallback } from 'react';
import { useSelector } from 'react-redux';
import { noop } from 'lodash';

import * as selectors from './selectors';
import { Entries } from './types';
import * as actions from './actions';
import { createSelector } from 'reselect';

import SimpleEditor from './editor/SimpleEditor';
import { useAppDispatch } from './configureStore';

const entriesToSaveSelector = createSelector(
  selectors.entriesSelector,
  (entries) => {
    const names = Object.keys(entries);
    const toBeSaved: Entries = {};
    for (const name of names) {
      const entry = entries[name];
      if (!entry.lastSaved || entry.lastEdited > entry.lastSaved) {
        toBeSaved[name] = entry;
      }
    }
    return toBeSaved;
  }
);

const entryNamesSelector = createSelector(selectors.entriesSelector, Object.keys);

const doSave = (): actions.ThunkAction => (dispatch, getState) => {
  const state = getState();
  const toSave = entriesToSaveSelector(state);
  dispatch(actions.saveFiles(toSave));
};

const MultiEditor: React.FC = () => {
  const entryNames = useSelector(entryNamesSelector);
  const entry = useSelector(selectors.currentEntrySelector);

  const toSave = useSelector(entriesToSaveSelector);
  const toSaveNames = Object.keys(toSave);

  const dispatch = useAppDispatch()

  const clickSave = useCallback(() => dispatch(doSave()), [dispatch]);

  return (
    <>
      <div>I am the multieditor</div>
      { toSaveNames.map(n => <b key={n}>{n}</b>) }
      <button onClick={clickSave}>Go Save Now</button>

      { entryNames.map((name) => (
        <button key={name} onClick={() => dispatch(actions.selectFile(name))}>{name}</button>
      )) }
      <SimpleEditor
        code={entry.code}
        execute={noop}
        onEditCode={newCode => dispatch(actions.editFile(entry.name, newCode))}
        position={entry.position}
        selection={entry.selection}
        crates={[]} />
    </>
  );
};

export default MultiEditor;
