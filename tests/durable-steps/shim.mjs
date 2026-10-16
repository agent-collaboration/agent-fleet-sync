import * as imports from "./index_bg.js";
import wasmModule from "./index_bg.wasm";
const instance = new WebAssembly.Instance(wasmModule, { "./index_bg.js": imports });
imports.__wbg_set_wasm(instance.exports);
instance.exports.__wbindgen_start?.();
export const StepsObject = imports.StepsObject;
export default { fetch: imports.fetch };
