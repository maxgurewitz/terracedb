"use strict";

const READONLY_VIEW_SCHEME = "terrace-view";

function normalizePath(path) {
  if (!path) {
    return "/";
  }
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return normalized.replace(/\/+/g, "/");
}

function toReadonlyUri(location) {
  const path = normalizePath(location.path).replace(/^\//, "");
  return `${READONLY_VIEW_SCHEME}://session/${location.session_volume_id}/${location.cut}/${path}`;
}

function parseReadonlyUri(uri) {
  const prefix = `${READONLY_VIEW_SCHEME}://session/`;
  if (!uri.startsWith(prefix)) {
    throw new Error(`Invalid readonly view URI: ${uri}`);
  }
  const rest = uri.slice(prefix.length);
  const [sessionVolumeId, cut, ...pathParts] = rest.split("/");
  if (!sessionVolumeId || !cut) {
    throw new Error(`Invalid readonly view URI: ${uri}`);
  }
  return {
    session_volume_id: sessionVolumeId,
    cut,
    path: normalizePath(pathParts.join("/"))
  };
}

module.exports = {
  READONLY_VIEW_SCHEME,
  normalizePath,
  parseReadonlyUri,
  toReadonlyUri
};
