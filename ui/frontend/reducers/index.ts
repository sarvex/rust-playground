import { combineReducers } from 'redux';

import browser from './browser';
import configuration from './configuration';
import crates from './crates';
import files from './files';
import globalConfiguration from './globalConfiguration';
import notifications from './notifications';
import output from './output';
import page from './page';
import versions from './versions';
import websocket from './websocket';

const playgroundApp = combineReducers({
  browser,
  configuration,
  crates,
  files,
  globalConfiguration,
  notifications,
  output,
  page,
  versions,
  websocket,
});

export type State = ReturnType<typeof playgroundApp>;

export default playgroundApp;
