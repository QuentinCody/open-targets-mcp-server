var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/_internal/utils.mjs
// @__NO_SIDE_EFFECTS__
function createNotImplementedError(name) {
  return new Error(`[unenv] ${name} is not implemented yet!`);
}
__name(createNotImplementedError, "createNotImplementedError");
// @__NO_SIDE_EFFECTS__
function notImplemented(name) {
  const fn = /* @__PURE__ */ __name(() => {
    throw /* @__PURE__ */ createNotImplementedError(name);
  }, "fn");
  return Object.assign(fn, { __unenv__: true });
}
__name(notImplemented, "notImplemented");
// @__NO_SIDE_EFFECTS__
function notImplementedClass(name) {
  return class {
    __unenv__ = true;
    constructor() {
      throw new Error(`[unenv] ${name} is not implemented yet!`);
    }
  };
}
__name(notImplementedClass, "notImplementedClass");

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/perf_hooks/performance.mjs
var _timeOrigin = globalThis.performance?.timeOrigin ?? Date.now();
var _performanceNow = globalThis.performance?.now ? globalThis.performance.now.bind(globalThis.performance) : () => Date.now() - _timeOrigin;
var nodeTiming = {
  name: "node",
  entryType: "node",
  startTime: 0,
  duration: 0,
  nodeStart: 0,
  v8Start: 0,
  bootstrapComplete: 0,
  environment: 0,
  loopStart: 0,
  loopExit: 0,
  idleTime: 0,
  uvMetricsInfo: {
    loopCount: 0,
    events: 0,
    eventsWaiting: 0
  },
  detail: void 0,
  toJSON() {
    return this;
  }
};
var PerformanceEntry = class {
  static {
    __name(this, "PerformanceEntry");
  }
  __unenv__ = true;
  detail;
  entryType = "event";
  name;
  startTime;
  constructor(name, options) {
    this.name = name;
    this.startTime = options?.startTime || _performanceNow();
    this.detail = options?.detail;
  }
  get duration() {
    return _performanceNow() - this.startTime;
  }
  toJSON() {
    return {
      name: this.name,
      entryType: this.entryType,
      startTime: this.startTime,
      duration: this.duration,
      detail: this.detail
    };
  }
};
var PerformanceMark = class PerformanceMark2 extends PerformanceEntry {
  static {
    __name(this, "PerformanceMark");
  }
  entryType = "mark";
  constructor() {
    super(...arguments);
  }
  get duration() {
    return 0;
  }
};
var PerformanceMeasure = class extends PerformanceEntry {
  static {
    __name(this, "PerformanceMeasure");
  }
  entryType = "measure";
};
var PerformanceResourceTiming = class extends PerformanceEntry {
  static {
    __name(this, "PerformanceResourceTiming");
  }
  entryType = "resource";
  serverTiming = [];
  connectEnd = 0;
  connectStart = 0;
  decodedBodySize = 0;
  domainLookupEnd = 0;
  domainLookupStart = 0;
  encodedBodySize = 0;
  fetchStart = 0;
  initiatorType = "";
  name = "";
  nextHopProtocol = "";
  redirectEnd = 0;
  redirectStart = 0;
  requestStart = 0;
  responseEnd = 0;
  responseStart = 0;
  secureConnectionStart = 0;
  startTime = 0;
  transferSize = 0;
  workerStart = 0;
  responseStatus = 0;
};
var PerformanceObserverEntryList = class {
  static {
    __name(this, "PerformanceObserverEntryList");
  }
  __unenv__ = true;
  getEntries() {
    return [];
  }
  getEntriesByName(_name, _type) {
    return [];
  }
  getEntriesByType(type) {
    return [];
  }
};
var Performance = class {
  static {
    __name(this, "Performance");
  }
  __unenv__ = true;
  timeOrigin = _timeOrigin;
  eventCounts = /* @__PURE__ */ new Map();
  _entries = [];
  _resourceTimingBufferSize = 0;
  navigation = void 0;
  timing = void 0;
  timerify(_fn, _options) {
    throw createNotImplementedError("Performance.timerify");
  }
  get nodeTiming() {
    return nodeTiming;
  }
  eventLoopUtilization() {
    return {};
  }
  markResourceTiming() {
    return new PerformanceResourceTiming("");
  }
  onresourcetimingbufferfull = null;
  now() {
    if (this.timeOrigin === _timeOrigin) {
      return _performanceNow();
    }
    return Date.now() - this.timeOrigin;
  }
  clearMarks(markName) {
    this._entries = markName ? this._entries.filter((e) => e.name !== markName) : this._entries.filter((e) => e.entryType !== "mark");
  }
  clearMeasures(measureName) {
    this._entries = measureName ? this._entries.filter((e) => e.name !== measureName) : this._entries.filter((e) => e.entryType !== "measure");
  }
  clearResourceTimings() {
    this._entries = this._entries.filter((e) => e.entryType !== "resource" || e.entryType !== "navigation");
  }
  getEntries() {
    return this._entries;
  }
  getEntriesByName(name, type) {
    return this._entries.filter((e) => e.name === name && (!type || e.entryType === type));
  }
  getEntriesByType(type) {
    return this._entries.filter((e) => e.entryType === type);
  }
  mark(name, options) {
    const entry = new PerformanceMark(name, options);
    this._entries.push(entry);
    return entry;
  }
  measure(measureName, startOrMeasureOptions, endMark) {
    let start;
    let end;
    if (typeof startOrMeasureOptions === "string") {
      start = this.getEntriesByName(startOrMeasureOptions, "mark")[0]?.startTime;
      end = this.getEntriesByName(endMark, "mark")[0]?.startTime;
    } else {
      start = Number.parseFloat(startOrMeasureOptions?.start) || this.now();
      end = Number.parseFloat(startOrMeasureOptions?.end) || this.now();
    }
    const entry = new PerformanceMeasure(measureName, {
      startTime: start,
      detail: {
        start,
        end
      }
    });
    this._entries.push(entry);
    return entry;
  }
  setResourceTimingBufferSize(maxSize) {
    this._resourceTimingBufferSize = maxSize;
  }
  addEventListener(type, listener, options) {
    throw createNotImplementedError("Performance.addEventListener");
  }
  removeEventListener(type, listener, options) {
    throw createNotImplementedError("Performance.removeEventListener");
  }
  dispatchEvent(event) {
    throw createNotImplementedError("Performance.dispatchEvent");
  }
  toJSON() {
    return this;
  }
};
var PerformanceObserver = class {
  static {
    __name(this, "PerformanceObserver");
  }
  __unenv__ = true;
  static supportedEntryTypes = [];
  _callback = null;
  constructor(callback) {
    this._callback = callback;
  }
  takeRecords() {
    return [];
  }
  disconnect() {
    throw createNotImplementedError("PerformanceObserver.disconnect");
  }
  observe(options) {
    throw createNotImplementedError("PerformanceObserver.observe");
  }
  bind(fn) {
    return fn;
  }
  runInAsyncScope(fn, thisArg, ...args) {
    return fn.call(thisArg, ...args);
  }
  asyncId() {
    return 0;
  }
  triggerAsyncId() {
    return 0;
  }
  emitDestroy() {
    return this;
  }
};
var performance = globalThis.performance && "addEventListener" in globalThis.performance ? globalThis.performance : new Performance();

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/polyfill/performance.mjs
globalThis.performance = performance;
globalThis.Performance = Performance;
globalThis.PerformanceEntry = PerformanceEntry;
globalThis.PerformanceMark = PerformanceMark;
globalThis.PerformanceMeasure = PerformanceMeasure;
globalThis.PerformanceObserver = PerformanceObserver;
globalThis.PerformanceObserverEntryList = PerformanceObserverEntryList;
globalThis.PerformanceResourceTiming = PerformanceResourceTiming;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/console.mjs
import { Writable } from "node:stream";

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/mock/noop.mjs
var noop_default = Object.assign(() => {
}, { __unenv__: true });

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/console.mjs
var _console = globalThis.console;
var _ignoreErrors = true;
var _stderr = new Writable();
var _stdout = new Writable();
var log = _console?.log ?? noop_default;
var info = _console?.info ?? log;
var trace = _console?.trace ?? info;
var debug = _console?.debug ?? log;
var table = _console?.table ?? log;
var error = _console?.error ?? log;
var warn = _console?.warn ?? error;
var createTask = _console?.createTask ?? /* @__PURE__ */ notImplemented("console.createTask");
var clear = _console?.clear ?? noop_default;
var count = _console?.count ?? noop_default;
var countReset = _console?.countReset ?? noop_default;
var dir = _console?.dir ?? noop_default;
var dirxml = _console?.dirxml ?? noop_default;
var group = _console?.group ?? noop_default;
var groupEnd = _console?.groupEnd ?? noop_default;
var groupCollapsed = _console?.groupCollapsed ?? noop_default;
var profile = _console?.profile ?? noop_default;
var profileEnd = _console?.profileEnd ?? noop_default;
var time = _console?.time ?? noop_default;
var timeEnd = _console?.timeEnd ?? noop_default;
var timeLog = _console?.timeLog ?? noop_default;
var timeStamp = _console?.timeStamp ?? noop_default;
var Console = _console?.Console ?? /* @__PURE__ */ notImplementedClass("console.Console");
var _times = /* @__PURE__ */ new Map();
var _stdoutErrorHandler = noop_default;
var _stderrErrorHandler = noop_default;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/node/console.mjs
var workerdConsole = globalThis["console"];
var {
  assert,
  clear: clear2,
  // @ts-expect-error undocumented public API
  context,
  count: count2,
  countReset: countReset2,
  // @ts-expect-error undocumented public API
  createTask: createTask2,
  debug: debug2,
  dir: dir2,
  dirxml: dirxml2,
  error: error2,
  group: group2,
  groupCollapsed: groupCollapsed2,
  groupEnd: groupEnd2,
  info: info2,
  log: log2,
  profile: profile2,
  profileEnd: profileEnd2,
  table: table2,
  time: time2,
  timeEnd: timeEnd2,
  timeLog: timeLog2,
  timeStamp: timeStamp2,
  trace: trace2,
  warn: warn2
} = workerdConsole;
Object.assign(workerdConsole, {
  Console,
  _ignoreErrors,
  _stderr,
  _stderrErrorHandler,
  _stdout,
  _stdoutErrorHandler,
  _times
});
var console_default = workerdConsole;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/_virtual_unenv_global_polyfill-@cloudflare-unenv-preset-node-console
globalThis.console = console_default;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/hrtime.mjs
var hrtime = /* @__PURE__ */ Object.assign(/* @__PURE__ */ __name(function hrtime2(startTime) {
  const now = Date.now();
  const seconds = Math.trunc(now / 1e3);
  const nanos = now % 1e3 * 1e6;
  if (startTime) {
    let diffSeconds = seconds - startTime[0];
    let diffNanos = nanos - startTime[0];
    if (diffNanos < 0) {
      diffSeconds = diffSeconds - 1;
      diffNanos = 1e9 + diffNanos;
    }
    return [diffSeconds, diffNanos];
  }
  return [seconds, nanos];
}, "hrtime"), { bigint: /* @__PURE__ */ __name(function bigint() {
  return BigInt(Date.now() * 1e6);
}, "bigint") });

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/process.mjs
import { EventEmitter } from "node:events";

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/write-stream.mjs
var WriteStream = class {
  static {
    __name(this, "WriteStream");
  }
  fd;
  columns = 80;
  rows = 24;
  isTTY = false;
  constructor(fd) {
    this.fd = fd;
  }
  clearLine(dir3, callback) {
    callback && callback();
    return false;
  }
  clearScreenDown(callback) {
    callback && callback();
    return false;
  }
  cursorTo(x, y, callback) {
    callback && typeof callback === "function" && callback();
    return false;
  }
  moveCursor(dx, dy, callback) {
    callback && callback();
    return false;
  }
  getColorDepth(env2) {
    return 1;
  }
  hasColors(count3, env2) {
    return false;
  }
  getWindowSize() {
    return [this.columns, this.rows];
  }
  write(str, encoding, cb) {
    if (str instanceof Uint8Array) {
      str = new TextDecoder().decode(str);
    }
    try {
      console.log(str);
    } catch {
    }
    cb && typeof cb === "function" && cb();
    return false;
  }
};

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/read-stream.mjs
var ReadStream = class {
  static {
    __name(this, "ReadStream");
  }
  fd;
  isRaw = false;
  isTTY = false;
  constructor(fd) {
    this.fd = fd;
  }
  setRawMode(mode) {
    this.isRaw = mode;
    return this;
  }
};

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/process.mjs
var Process = class _Process extends EventEmitter {
  static {
    __name(this, "Process");
  }
  env;
  hrtime;
  nextTick;
  constructor(impl) {
    super();
    this.env = impl.env;
    this.hrtime = impl.hrtime;
    this.nextTick = impl.nextTick;
    for (const prop of [...Object.getOwnPropertyNames(_Process.prototype), ...Object.getOwnPropertyNames(EventEmitter.prototype)]) {
      const value = this[prop];
      if (typeof value === "function") {
        this[prop] = value.bind(this);
      }
    }
  }
  emitWarning(warning, type, code) {
    console.warn(`${code ? `[${code}] ` : ""}${type ? `${type}: ` : ""}${warning}`);
  }
  emit(...args) {
    return super.emit(...args);
  }
  listeners(eventName) {
    return super.listeners(eventName);
  }
  #stdin;
  #stdout;
  #stderr;
  get stdin() {
    return this.#stdin ??= new ReadStream(0);
  }
  get stdout() {
    return this.#stdout ??= new WriteStream(1);
  }
  get stderr() {
    return this.#stderr ??= new WriteStream(2);
  }
  #cwd = "/";
  chdir(cwd2) {
    this.#cwd = cwd2;
  }
  cwd() {
    return this.#cwd;
  }
  arch = "";
  platform = "";
  argv = [];
  argv0 = "";
  execArgv = [];
  execPath = "";
  title = "";
  pid = 200;
  ppid = 100;
  get version() {
    return "";
  }
  get versions() {
    return {};
  }
  get allowedNodeEnvironmentFlags() {
    return /* @__PURE__ */ new Set();
  }
  get sourceMapsEnabled() {
    return false;
  }
  get debugPort() {
    return 0;
  }
  get throwDeprecation() {
    return false;
  }
  get traceDeprecation() {
    return false;
  }
  get features() {
    return {};
  }
  get release() {
    return {};
  }
  get connected() {
    return false;
  }
  get config() {
    return {};
  }
  get moduleLoadList() {
    return [];
  }
  constrainedMemory() {
    return 0;
  }
  availableMemory() {
    return 0;
  }
  uptime() {
    return 0;
  }
  resourceUsage() {
    return {};
  }
  ref() {
  }
  unref() {
  }
  umask() {
    throw createNotImplementedError("process.umask");
  }
  getBuiltinModule() {
    return void 0;
  }
  getActiveResourcesInfo() {
    throw createNotImplementedError("process.getActiveResourcesInfo");
  }
  exit() {
    throw createNotImplementedError("process.exit");
  }
  reallyExit() {
    throw createNotImplementedError("process.reallyExit");
  }
  kill() {
    throw createNotImplementedError("process.kill");
  }
  abort() {
    throw createNotImplementedError("process.abort");
  }
  dlopen() {
    throw createNotImplementedError("process.dlopen");
  }
  setSourceMapsEnabled() {
    throw createNotImplementedError("process.setSourceMapsEnabled");
  }
  loadEnvFile() {
    throw createNotImplementedError("process.loadEnvFile");
  }
  disconnect() {
    throw createNotImplementedError("process.disconnect");
  }
  cpuUsage() {
    throw createNotImplementedError("process.cpuUsage");
  }
  setUncaughtExceptionCaptureCallback() {
    throw createNotImplementedError("process.setUncaughtExceptionCaptureCallback");
  }
  hasUncaughtExceptionCaptureCallback() {
    throw createNotImplementedError("process.hasUncaughtExceptionCaptureCallback");
  }
  initgroups() {
    throw createNotImplementedError("process.initgroups");
  }
  openStdin() {
    throw createNotImplementedError("process.openStdin");
  }
  assert() {
    throw createNotImplementedError("process.assert");
  }
  binding() {
    throw createNotImplementedError("process.binding");
  }
  permission = { has: /* @__PURE__ */ notImplemented("process.permission.has") };
  report = {
    directory: "",
    filename: "",
    signal: "SIGUSR2",
    compact: false,
    reportOnFatalError: false,
    reportOnSignal: false,
    reportOnUncaughtException: false,
    getReport: /* @__PURE__ */ notImplemented("process.report.getReport"),
    writeReport: /* @__PURE__ */ notImplemented("process.report.writeReport")
  };
  finalization = {
    register: /* @__PURE__ */ notImplemented("process.finalization.register"),
    unregister: /* @__PURE__ */ notImplemented("process.finalization.unregister"),
    registerBeforeExit: /* @__PURE__ */ notImplemented("process.finalization.registerBeforeExit")
  };
  memoryUsage = Object.assign(() => ({
    arrayBuffers: 0,
    rss: 0,
    external: 0,
    heapTotal: 0,
    heapUsed: 0
  }), { rss: /* @__PURE__ */ __name(() => 0, "rss") });
  mainModule = void 0;
  domain = void 0;
  send = void 0;
  exitCode = void 0;
  channel = void 0;
  getegid = void 0;
  geteuid = void 0;
  getgid = void 0;
  getgroups = void 0;
  getuid = void 0;
  setegid = void 0;
  seteuid = void 0;
  setgid = void 0;
  setgroups = void 0;
  setuid = void 0;
  _events = void 0;
  _eventsCount = void 0;
  _exiting = void 0;
  _maxListeners = void 0;
  _debugEnd = void 0;
  _debugProcess = void 0;
  _fatalException = void 0;
  _getActiveHandles = void 0;
  _getActiveRequests = void 0;
  _kill = void 0;
  _preload_modules = void 0;
  _rawDebug = void 0;
  _startProfilerIdleNotifier = void 0;
  _stopProfilerIdleNotifier = void 0;
  _tickCallback = void 0;
  _disconnect = void 0;
  _handleQueue = void 0;
  _pendingMessage = void 0;
  _channel = void 0;
  _send = void 0;
  _linkedBinding = void 0;
};

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/node/process.mjs
var globalProcess = globalThis["process"];
var getBuiltinModule = globalProcess.getBuiltinModule;
var { exit, platform, nextTick } = getBuiltinModule(
  "node:process"
);
var unenvProcess = new Process({
  env: globalProcess.env,
  hrtime,
  nextTick
});
var {
  abort,
  addListener,
  allowedNodeEnvironmentFlags,
  hasUncaughtExceptionCaptureCallback,
  setUncaughtExceptionCaptureCallback,
  loadEnvFile,
  sourceMapsEnabled,
  arch,
  argv,
  argv0,
  chdir,
  config,
  connected,
  constrainedMemory,
  availableMemory,
  cpuUsage,
  cwd,
  debugPort,
  dlopen,
  disconnect,
  emit,
  emitWarning,
  env,
  eventNames,
  execArgv,
  execPath,
  finalization,
  features,
  getActiveResourcesInfo,
  getMaxListeners,
  hrtime: hrtime3,
  kill,
  listeners,
  listenerCount,
  memoryUsage,
  on,
  off,
  once,
  pid,
  ppid,
  prependListener,
  prependOnceListener,
  rawListeners,
  release,
  removeAllListeners,
  removeListener,
  report,
  resourceUsage,
  setMaxListeners,
  setSourceMapsEnabled,
  stderr,
  stdin,
  stdout,
  title,
  throwDeprecation,
  traceDeprecation,
  umask,
  uptime,
  version,
  versions,
  domain,
  initgroups,
  moduleLoadList,
  reallyExit,
  openStdin,
  assert: assert2,
  binding,
  send,
  exitCode,
  channel,
  getegid,
  geteuid,
  getgid,
  getgroups,
  getuid,
  setegid,
  seteuid,
  setgid,
  setgroups,
  setuid,
  permission,
  mainModule,
  _events,
  _eventsCount,
  _exiting,
  _maxListeners,
  _debugEnd,
  _debugProcess,
  _fatalException,
  _getActiveHandles,
  _getActiveRequests,
  _kill,
  _preload_modules,
  _rawDebug,
  _startProfilerIdleNotifier,
  _stopProfilerIdleNotifier,
  _tickCallback,
  _disconnect,
  _handleQueue,
  _pendingMessage,
  _channel,
  _send,
  _linkedBinding
} = unenvProcess;
var _process = {
  abort,
  addListener,
  allowedNodeEnvironmentFlags,
  hasUncaughtExceptionCaptureCallback,
  setUncaughtExceptionCaptureCallback,
  loadEnvFile,
  sourceMapsEnabled,
  arch,
  argv,
  argv0,
  chdir,
  config,
  connected,
  constrainedMemory,
  availableMemory,
  cpuUsage,
  cwd,
  debugPort,
  dlopen,
  disconnect,
  emit,
  emitWarning,
  env,
  eventNames,
  execArgv,
  execPath,
  exit,
  finalization,
  features,
  getBuiltinModule,
  getActiveResourcesInfo,
  getMaxListeners,
  hrtime: hrtime3,
  kill,
  listeners,
  listenerCount,
  memoryUsage,
  nextTick,
  on,
  off,
  once,
  pid,
  platform,
  ppid,
  prependListener,
  prependOnceListener,
  rawListeners,
  release,
  removeAllListeners,
  removeListener,
  report,
  resourceUsage,
  setMaxListeners,
  setSourceMapsEnabled,
  stderr,
  stdin,
  stdout,
  title,
  throwDeprecation,
  traceDeprecation,
  umask,
  uptime,
  version,
  versions,
  // @ts-expect-error old API
  domain,
  initgroups,
  moduleLoadList,
  reallyExit,
  openStdin,
  assert: assert2,
  binding,
  send,
  exitCode,
  channel,
  getegid,
  geteuid,
  getgid,
  getgroups,
  getuid,
  setegid,
  seteuid,
  setgid,
  setgroups,
  setuid,
  permission,
  mainModule,
  _events,
  _eventsCount,
  _exiting,
  _maxListeners,
  _debugEnd,
  _debugProcess,
  _fatalException,
  _getActiveHandles,
  _getActiveRequests,
  _kill,
  _preload_modules,
  _rawDebug,
  _startProfilerIdleNotifier,
  _stopProfilerIdleNotifier,
  _tickCallback,
  _disconnect,
  _handleQueue,
  _pendingMessage,
  _channel,
  _send,
  _linkedBinding
};
var process_default = _process;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/_virtual_unenv_global_polyfill-@cloudflare-unenv-preset-node-process
globalThis.process = process_default;

// src/do.ts
import { DurableObject } from "cloudflare:workers";

// src/lib/ChunkingEngine.ts
var ChunkingEngine = class {
  constructor() {
    this.CHUNK_SIZE_THRESHOLD = 32 * 1024;
    // 32KB - configurable
    this.CHUNK_SIZE = 16 * 1024;
    // 16KB per chunk - optimal for SQLite
    this.ENABLE_COMPRESSION = true;
    this.chunkingRules = [];
  }
  static {
    __name(this, "ChunkingEngine");
  }
  /**
   * Configure schema-aware chunking
   */
  configureSchemaAwareness(schemaInfo) {
    this.schemaInfo = schemaInfo;
    this.generateOpenTargetsChunkingRules();
  }
  /**
   * Generate intelligent chunking rules based on Open Targets schema patterns
   */
  generateOpenTargetsChunkingRules() {
    this.chunkingRules = [
      // Base rules that apply to all types
      { fieldName: "id", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "ID fields should never be chunked" },
      { fieldName: "ensemblId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "Ensembl ID fields should never be chunked" },
      { fieldName: "efoId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "EFO ID fields should never be chunked" },
      { fieldName: "chemblId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "ChEMBL ID fields should never be chunked" },
      // Open Targets-specific large content fields
      { fieldName: "description", typeName: "Target", chunkThreshold: 2048, priority: "always", reason: "Target descriptions are typically very long" },
      { fieldName: "description", typeName: "Disease", chunkThreshold: 2048, priority: "always", reason: "Disease descriptions can be extensive" },
      { fieldName: "description", typeName: "Drug", chunkThreshold: 2048, priority: "always", reason: "Drug descriptions can be extensive" },
      { fieldName: "synonyms", typeName: "*", chunkThreshold: 1024, priority: "size-based", reason: "Synonym arrays can be large" },
      // Tractability and constraint data
      { fieldName: "tractability", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Tractability data contains extensive nested information" },
      { fieldName: "geneticConstraint", typeName: "Target", chunkThreshold: 2048, priority: "size-based", reason: "Genetic constraint data can be detailed" },
      { fieldName: "safety", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Safety information can be extensive" },
      // Association and evidence data
      { fieldName: "evidences", typeName: "*", chunkThreshold: 8192, priority: "size-based", reason: "Evidence arrays can be very large" },
      { fieldName: "associations", typeName: "*", chunkThreshold: 8192, priority: "size-based", reason: "Association arrays can be very large" },
      { fieldName: "studies", typeName: "*", chunkThreshold: 6144, priority: "size-based", reason: "Study arrays can be extensive" },
      // Pharmacovigilance and drug data
      { fieldName: "pharmacovigilance", typeName: "Drug", chunkThreshold: 8192, priority: "size-based", reason: "Pharmacovigilance data can be very large" },
      { fieldName: "mechanismsOfAction", typeName: "Drug", chunkThreshold: 4096, priority: "size-based", reason: "Mechanism of action data can be extensive" },
      { fieldName: "indications", typeName: "Drug", chunkThreshold: 4096, priority: "size-based", reason: "Drug indications can be numerous" },
      // Ontology and classification data
      { fieldName: "ontology", typeName: "Disease", chunkThreshold: 4096, priority: "size-based", reason: "Ontology data can contain extensive hierarchical information" },
      { fieldName: "therapeuticAreas", typeName: "Disease", chunkThreshold: 2048, priority: "size-based", reason: "Therapeutic area data can be extensive" },
      // Expression and interaction data
      { fieldName: "expressions", typeName: "Target", chunkThreshold: 6144, priority: "size-based", reason: "Expression data can be extensive across tissues" },
      { fieldName: "interactions", typeName: "Target", chunkThreshold: 6144, priority: "size-based", reason: "Interaction data can be extensive" },
      { fieldName: "pathways", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Pathway data can be extensive" },
      // Conservative chunking for names and identifiers
      { fieldName: "approvedName", typeName: "*", chunkThreshold: 512, priority: "size-based", reason: "Approved names are usually short but can be long" },
      { fieldName: "approvedSymbol", typeName: "*", chunkThreshold: 256, priority: "size-based", reason: "Symbols are usually short" },
      { fieldName: "name", typeName: "*", chunkThreshold: 512, priority: "size-based", reason: "Names are usually short but can be long" }
    ];
    if (this.schemaInfo) {
      for (const [typeName, typeInfo] of Object.entries(this.schemaInfo.types)) {
        if (typeInfo.kind === "OBJECT") {
          for (const [fieldName, fieldInfo] of Object.entries(typeInfo.fields)) {
            if (fieldInfo.isList && this.isLikelyLargeContent(fieldInfo)) {
              this.chunkingRules.push({
                fieldName,
                typeName,
                chunkThreshold: 8192,
                priority: "size-based",
                reason: `List field ${fieldName} on ${typeName} likely contains large content`
              });
            }
          }
        }
      }
    }
  }
  /**
   * Determine if a field is likely to contain large content based on Open Targets patterns
   */
  isLikelyLargeContent(fieldInfo) {
    const largeContentIndicators = [
      "description",
      "summary",
      "evidence",
      "associations",
      "studies",
      "tractability",
      "constraint",
      "safety",
      "pharmacovigilance",
      "mechanisms",
      "indications",
      "ontology",
      "expressions",
      "interactions",
      "pathways",
      "therapeuticAreas",
      "synonyms",
      "alternativeNames"
    ];
    return largeContentIndicators.some(
      (indicator) => fieldInfo.name.toLowerCase().includes(indicator) || fieldInfo.description?.toLowerCase().includes(indicator)
    );
  }
  /**
   * Schema-aware JSON stringification with intelligent chunking decisions
   */
  async schemaAwareJsonStringify(obj, typeName, fieldName, sql) {
    const jsonString = JSON.stringify(obj);
    const applicableRule = this.getApplicableChunkingRule(fieldName, typeName);
    if (applicableRule) {
      if (applicableRule.priority === "never") {
        return jsonString;
      } else if (applicableRule.priority === "always" && jsonString.length > applicableRule.chunkThreshold) {
        const metadata2 = await this.storeChunkedContent(jsonString, "json", sql);
        return this.createContentReference(metadata2);
      } else if (applicableRule.priority === "size-based" && jsonString.length > applicableRule.chunkThreshold) {
        const metadata2 = await this.storeChunkedContent(jsonString, "json", sql);
        return this.createContentReference(metadata2);
      }
    }
    if (!this.shouldChunk(jsonString)) {
      return jsonString;
    }
    const metadata = await this.storeChunkedContent(jsonString, "json", sql);
    return this.createContentReference(metadata);
  }
  /**
   * Get the most specific chunking rule for a field
   */
  getApplicableChunkingRule(fieldName, typeName) {
    let rule = this.chunkingRules.find((r) => r.fieldName === fieldName && r.typeName === typeName);
    if (rule) return rule;
    rule = this.chunkingRules.find((r) => r.fieldName === fieldName && r.typeName === "*");
    if (rule) return rule;
    return null;
  }
  /**
   * Determines if content should be chunked based on size threshold
   */
  shouldChunk(content) {
    return content.length > this.CHUNK_SIZE_THRESHOLD;
  }
  /**
   * Stores large content as chunks, returns metadata for retrieval
   */
  async storeChunkedContent(content, contentType, sql) {
    const contentId = this.generateContentId();
    let processedContent = content;
    let compressed = false;
    if (this.ENABLE_COMPRESSION && this.shouldCompress(content)) {
      try {
        processedContent = await this.compress(content);
        compressed = true;
      } catch (error3) {
        console.warn("Compression failed, storing uncompressed:", error3);
        processedContent = content;
      }
    }
    await this.ensureChunksTable(sql);
    const chunks = this.splitIntoChunks(processedContent);
    for (let i = 0; i < chunks.length; i++) {
      const chunkRecord = {
        content_id: contentId,
        chunk_index: i,
        chunk_data: chunks[i],
        chunk_size: chunks[i].length
      };
      await this.insertChunk(chunkRecord, sql);
    }
    const metadata = {
      contentId,
      totalChunks: chunks.length,
      originalSize: content.length,
      contentType,
      compressed,
      encoding: compressed ? "gzip" : void 0
    };
    await this.storeMetadata(metadata, sql);
    return metadata;
  }
  /**
   * Retrieves and reassembles chunked content
   */
  async retrieveChunkedContent(contentId, sql) {
    try {
      const metadata = await this.getMetadata(contentId, sql);
      if (!metadata) return null;
      const chunks = await this.getChunks(contentId, metadata.totalChunks, sql);
      if (chunks.length !== metadata.totalChunks) {
        throw new Error(`Missing chunks: expected ${metadata.totalChunks}, found ${chunks.length}`);
      }
      const reassembled = chunks.join("");
      if (metadata.compressed) {
        try {
          return await this.decompress(reassembled);
        } catch (error3) {
          console.error("Decompression failed:", error3);
          throw new Error("Failed to decompress content");
        }
      }
      return reassembled;
    } catch (error3) {
      console.error(`Failed to retrieve chunked content ${contentId}:`, error3);
      return null;
    }
  }
  /**
   * Creates a content reference for schema columns instead of storing large content directly
   */
  createContentReference(metadata) {
    return `__CHUNKED__:${metadata.contentId}`;
  }
  /**
   * Checks if a value is a chunked content reference
   */
  isContentReference(value) {
    return typeof value === "string" && value.startsWith("__CHUNKED__:");
  }
  /**
   * Extracts content ID from a content reference
   */
  extractContentId(reference) {
    return reference.replace("__CHUNKED__:", "");
  }
  /**
   * Enhanced JSON stringification with automatic chunking
   */
  async smartJsonStringify(obj, sql) {
    const jsonString = JSON.stringify(obj);
    if (!this.shouldChunk(jsonString)) {
      return jsonString;
    }
    const metadata = await this.storeChunkedContent(jsonString, "json", sql);
    return this.createContentReference(metadata);
  }
  /**
   * Enhanced JSON parsing with automatic chunk retrieval
   */
  async smartJsonParse(value, sql) {
    if (!this.isContentReference(value)) {
      return JSON.parse(value);
    }
    const contentId = this.extractContentId(value);
    const retrievedContent = await this.retrieveChunkedContent(contentId, sql);
    if (!retrievedContent) {
      throw new Error(`Failed to retrieve chunked content: ${contentId}`);
    }
    return JSON.parse(retrievedContent);
  }
  /**
   * Cleanup chunked content (for maintenance)
   */
  async cleanupChunkedContent(contentId, sql) {
    try {
      sql.exec(
        `DELETE FROM content_chunks WHERE content_id = ?`,
        contentId
      );
      sql.exec(
        `DELETE FROM chunk_metadata WHERE content_id = ?`,
        contentId
      );
    } catch (error3) {
      console.error(`Failed to cleanup chunked content ${contentId}:`, error3);
    }
  }
  /**
   * Get statistics about chunked content storage
   */
  async getChunkingStats(sql) {
    try {
      const metadataResult = sql.exec(`
				SELECT 
					COUNT(*) as total_chunked_items,
					SUM(original_size) as total_original_size,
					AVG(original_size) as avg_original_size,
					SUM(total_chunks) as total_chunks,
					COUNT(CASE WHEN compressed = 1 THEN 1 END) as compressed_items
				FROM chunk_metadata
			`).one();
      const chunksResult = sql.exec(`
				SELECT 
					COUNT(*) as total_chunk_records,
					SUM(chunk_size) as total_stored_size,
					AVG(chunk_size) as avg_chunk_size
				FROM content_chunks
			`).one();
      return {
        metadata: metadataResult || {},
        chunks: chunksResult || {},
        compression_ratio: metadataResult?.total_original_size && chunksResult?.total_stored_size ? (metadataResult.total_original_size / chunksResult.total_stored_size).toFixed(2) : null
      };
    } catch (error3) {
      return { error: error3 instanceof Error ? error3.message : "Failed to get stats" };
    }
  }
  /**
   * Analyze chunking effectiveness and provide Open Targets-specific recommendations
   */
  async analyzeChunkingEffectiveness(sql) {
    const stats = await this.getChunkingStats(sql);
    if (!this.schemaInfo) {
      return {
        ...stats,
        recommendation: "Enable schema-aware chunking by providing Open Targets GraphQL schema",
        schema_awareness: false
      };
    }
    const fieldAnalysis = await this.analyzeChunkedFields(sql);
    return {
      ...stats,
      schema_awareness: true,
      field_analysis: fieldAnalysis,
      recommendations: this.generateOpenTargetsRecommendations(fieldAnalysis)
    };
  }
  async analyzeChunkedFields(sql) {
    try {
      const result = sql.exec(`
				SELECT 
					content_type,
					original_size,
					compressed,
					COUNT(*) as chunk_count
				FROM chunk_metadata 
				GROUP BY content_type, compressed
				ORDER BY chunk_count DESC
			`).toArray();
      return result;
    } catch (error3) {
      return { error: "Could not analyze chunked fields" };
    }
  }
  generateOpenTargetsRecommendations(fieldAnalysis) {
    const recommendations = [];
    if (this.chunkingRules.length === 0) {
      recommendations.push("Configure Open Targets-specific chunking rules based on your data patterns");
    }
    if (fieldAnalysis && fieldAnalysis.length > 0) {
      const uncompressedCount = fieldAnalysis.filter((f) => !f.compressed).length;
      if (uncompressedCount > 0) {
        recommendations.push("Enable compression for better storage efficiency of biomedical data");
      }
    }
    recommendations.push("Monitor chunk size distribution for target/disease/drug association queries");
    recommendations.push("Consider pagination for large association queries to optimize performance");
    return recommendations;
  }
  // Private helper methods
  generateContentId() {
    return "chunk_" + crypto.randomUUID().replace(/-/g, "");
  }
  shouldCompress(content) {
    return content.length > 8192;
  }
  async compress(content) {
    try {
      const uint8Array = new TextEncoder().encode(content);
      if (typeof CompressionStream !== "undefined") {
        const compressionStream = new CompressionStream("gzip");
        const writer = compressionStream.writable.getWriter();
        const reader = compressionStream.readable.getReader();
        writer.write(uint8Array);
        writer.close();
        const chunks = [];
        let result = await reader.read();
        while (!result.done) {
          chunks.push(result.value);
          result = await reader.read();
        }
        const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
        const combined = new Uint8Array(totalLength);
        let offset = 0;
        for (const chunk of chunks) {
          combined.set(chunk, offset);
          offset += chunk.length;
        }
        return btoa(String.fromCharCode(...combined));
      }
      return btoa(content);
    } catch (error3) {
      throw new Error(`Compression failed: ${error3}`);
    }
  }
  async decompress(compressedContent) {
    try {
      if (typeof DecompressionStream !== "undefined") {
        const compressedData = Uint8Array.from(atob(compressedContent), (c) => c.charCodeAt(0));
        const decompressionStream = new DecompressionStream("gzip");
        const writer = decompressionStream.writable.getWriter();
        const reader = decompressionStream.readable.getReader();
        writer.write(compressedData);
        writer.close();
        const chunks = [];
        let result = await reader.read();
        while (!result.done) {
          chunks.push(result.value);
          result = await reader.read();
        }
        const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
        const combined = new Uint8Array(totalLength);
        let offset = 0;
        for (const chunk of chunks) {
          combined.set(chunk, offset);
          offset += chunk.length;
        }
        return new TextDecoder().decode(combined);
      }
      return atob(compressedContent);
    } catch (error3) {
      throw new Error(`Decompression failed: ${error3}`);
    }
  }
  splitIntoChunks(content) {
    const chunks = [];
    for (let i = 0; i < content.length; i += this.CHUNK_SIZE) {
      chunks.push(content.slice(i, i + this.CHUNK_SIZE));
    }
    return chunks;
  }
  async ensureChunksTable(sql) {
    sql.exec(`
			CREATE TABLE IF NOT EXISTS content_chunks (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				content_id TEXT NOT NULL,
				chunk_index INTEGER NOT NULL,
				chunk_data TEXT NOT NULL,
				chunk_size INTEGER NOT NULL,
				created_at TEXT DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(content_id, chunk_index)
			)
		`);
    sql.exec(`
			CREATE TABLE IF NOT EXISTS chunk_metadata (
				content_id TEXT PRIMARY KEY,
				total_chunks INTEGER NOT NULL,
				original_size INTEGER NOT NULL,
				content_type TEXT NOT NULL,
				compressed INTEGER DEFAULT 0,
				encoding TEXT,
				created_at TEXT DEFAULT CURRENT_TIMESTAMP
			)
		`);
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_content_chunks_lookup ON content_chunks(content_id, chunk_index)`);
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_chunk_metadata_size ON chunk_metadata(original_size)`);
  }
  async insertChunk(chunk, sql) {
    sql.exec(
      `INSERT INTO content_chunks (content_id, chunk_index, chunk_data, chunk_size) 
			 VALUES (?, ?, ?, ?)`,
      chunk.content_id,
      chunk.chunk_index,
      chunk.chunk_data,
      chunk.chunk_size
    );
  }
  async storeMetadata(metadata, sql) {
    sql.exec(
      `INSERT INTO chunk_metadata (content_id, total_chunks, original_size, content_type, compressed, encoding)
			 VALUES (?, ?, ?, ?, ?, ?)`,
      metadata.contentId,
      metadata.totalChunks,
      metadata.originalSize,
      metadata.contentType,
      metadata.compressed ? 1 : 0,
      metadata.encoding || null
    );
  }
  async getMetadata(contentId, sql) {
    const result = sql.exec(
      `SELECT * FROM chunk_metadata WHERE content_id = ?`,
      contentId
    ).one();
    if (!result) return null;
    return {
      contentId: result.content_id,
      totalChunks: result.total_chunks,
      originalSize: result.original_size,
      contentType: result.content_type,
      compressed: Boolean(result.compressed),
      encoding: result.encoding
    };
  }
  async getChunks(contentId, expectedCount, sql) {
    const results = sql.exec(
      `SELECT chunk_data FROM content_chunks 
			 WHERE content_id = ? 
			 ORDER BY chunk_index ASC`,
      contentId
    ).toArray();
    return results.map((row) => row.chunk_data);
  }
};

// src/lib/SchemaInferenceEngine.ts
var SchemaInferenceEngine = class {
  constructor() {
    this.chunkingEngine = new ChunkingEngine();
    this.discoveredEntities = /* @__PURE__ */ new Map();
    this.entityRelationships = /* @__PURE__ */ new Map();
  }
  static {
    __name(this, "SchemaInferenceEngine");
  }
  // Now tracks unique relationships only
  inferFromJSON(data) {
    this.discoveredEntities.clear();
    this.entityRelationships.clear();
    const schemas = {};
    this.discoverEntities(data, []);
    if (this.discoveredEntities.size > 0) {
      this.createSchemasFromEntities(schemas);
    } else {
      if (typeof data !== "object" || data === null || Array.isArray(data)) {
        const tableName = Array.isArray(data) ? "array_data" : "scalar_data";
        schemas[tableName] = this.createSchemaFromPrimitiveOrSimpleArray(data, tableName);
      } else {
        schemas.root_object = this.createSchemaFromObject(data, "root_object");
      }
    }
    return schemas;
  }
  discoverEntities(obj, path, parentEntityType) {
    if (!obj || typeof obj !== "object") {
      return;
    }
    if (Array.isArray(obj)) {
      if (obj.length > 0) {
        let arrayEntityType = null;
        for (const item of obj) {
          if (this.isEntity(item)) {
            if (!arrayEntityType) {
              arrayEntityType = this.inferEntityType(item, path);
            }
            const entitiesOfType = this.discoveredEntities.get(arrayEntityType) || [];
            entitiesOfType.push(item);
            this.discoveredEntities.set(arrayEntityType, entitiesOfType);
            if (parentEntityType && path.length > 0) {
              const fieldName = path[path.length - 1];
              if (fieldName !== "nodes" && fieldName !== "edges") {
                this.recordRelationship(parentEntityType, arrayEntityType);
              }
            }
            this.processEntityProperties(item, arrayEntityType);
          }
        }
      }
      return;
    }
    if (obj.edges && Array.isArray(obj.edges)) {
      const nodes = obj.edges.map((edge) => edge.node).filter(Boolean);
      if (nodes.length > 0) {
        this.discoverEntities(nodes, path, parentEntityType);
      }
      return;
    }
    if (obj.rows && Array.isArray(obj.rows)) {
      this.discoverEntities(obj.rows, path, parentEntityType);
      return;
    }
    if (this.isEntity(obj)) {
      const entityType = this.inferEntityType(obj, path);
      const entitiesOfType = this.discoveredEntities.get(entityType) || [];
      entitiesOfType.push(obj);
      this.discoveredEntities.set(entityType, entitiesOfType);
      this.processEntityProperties(obj, entityType);
      return;
    }
    for (const [key, value] of Object.entries(obj)) {
      this.discoverEntities(value, [...path, key], parentEntityType);
    }
  }
  processEntityProperties(entity, entityType) {
    for (const [key, value] of Object.entries(entity)) {
      if (Array.isArray(value) && value.length > 0) {
        const firstItem = value.find((item) => this.isEntity(item));
        if (firstItem) {
          const relatedEntityType = this.inferEntityType(firstItem, [key]);
          this.recordRelationship(entityType, relatedEntityType);
          value.forEach((item) => {
            if (this.isEntity(item)) {
              const entitiesOfType = this.discoveredEntities.get(relatedEntityType) || [];
              entitiesOfType.push(item);
              this.discoveredEntities.set(relatedEntityType, entitiesOfType);
              this.processEntityProperties(item, relatedEntityType);
            }
          });
        }
      } else if (value && typeof value === "object" && this.isEntity(value)) {
        const relatedEntityType = this.inferEntityType(value, [key]);
        this.recordRelationship(entityType, relatedEntityType);
        const entitiesOfType = this.discoveredEntities.get(relatedEntityType) || [];
        entitiesOfType.push(value);
        this.discoveredEntities.set(relatedEntityType, entitiesOfType);
        this.processEntityProperties(value, relatedEntityType);
      }
    }
  }
  isEntity(obj) {
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) return false;
    const hasId = obj.id !== void 0 || obj._id !== void 0 || obj.ensemblId !== void 0 || obj.efoId !== void 0 || obj.chemblId !== void 0;
    const fieldCount = Object.keys(obj).length;
    const hasMultipleFields = fieldCount >= 2;
    const hasEntityFields = obj.name !== void 0 || obj.approvedSymbol !== void 0 || obj.description !== void 0 || obj.type !== void 0 || obj.score !== void 0;
    return hasId || hasMultipleFields && hasEntityFields;
  }
  inferEntityType(obj, path) {
    if (obj.__typename) return this.sanitizeTableName(obj.__typename);
    if (obj.type && typeof obj.type === "string" && !["edges", "node"].includes(obj.type.toLowerCase())) {
      return this.sanitizeTableName(obj.type);
    }
    if (obj.ensemblId) return "target";
    if (obj.efoId) return "disease";
    if (obj.chemblId) return "drug";
    if (obj.approvedSymbol) return "target";
    if (path.length > 0) {
      let lastName = path[path.length - 1];
      if (lastName === "node" && path.length > 1) {
        lastName = path[path.length - 2];
        if (lastName === "edges" && path.length > 2) {
          lastName = path[path.length - 3];
        }
      } else if (lastName === "edges" && path.length > 1) {
        lastName = path[path.length - 2];
      } else if (lastName === "rows" && path.length > 1) {
        lastName = path[path.length - 2];
      }
      const sanitized = this.sanitizeTableName(lastName);
      if (sanitized.endsWith("ies")) {
        return sanitized.slice(0, -3) + "y";
      } else if (sanitized.endsWith("s") && !sanitized.endsWith("ss") && sanitized.length > 1) {
        const potentialSingular = sanitized.slice(0, -1);
        if (potentialSingular.length > 1) return potentialSingular;
      }
      return sanitized;
    }
    return "entity_" + Math.random().toString(36).substr(2, 9);
  }
  recordRelationship(fromTable, toTable) {
    if (fromTable === toTable) return;
    const relationshipKey = `${fromTable}_${toTable}`;
    const reverseKey = `${toTable}_${fromTable}`;
    const fromRelationships = this.entityRelationships.get(fromTable) || /* @__PURE__ */ new Set();
    const toRelationships = this.entityRelationships.get(toTable) || /* @__PURE__ */ new Set();
    if (!fromRelationships.has(toTable) && !toRelationships.has(fromTable)) {
      fromRelationships.add(toTable);
      this.entityRelationships.set(fromTable, fromRelationships);
    }
  }
  createSchemasFromEntities(schemas) {
    for (const [entityType, entities] of this.discoveredEntities.entries()) {
      if (entities.length === 0) continue;
      const columnTypes = {};
      const sampleData = [];
      entities.forEach((entity, index) => {
        if (index < 3) {
          sampleData.push(this.extractEntityFields(entity, columnTypes, entityType));
        } else {
          this.extractEntityFields(entity, columnTypes, entityType);
        }
      });
      const columns = this.resolveColumnTypes(columnTypes);
      this.ensureIdColumn(columns);
      schemas[entityType] = {
        columns,
        sample_data: sampleData
      };
    }
    this.createJunctionTableSchemas(schemas);
  }
  extractEntityFields(obj, columnTypes, entityType) {
    const rowData = {};
    if (!obj || typeof obj !== "object") {
      this.addColumnType(columnTypes, "value", this.getSQLiteType(obj));
      return { value: obj };
    }
    for (const [key, value] of Object.entries(obj)) {
      const columnName = this.sanitizeColumnName(key);
      if (Array.isArray(value)) {
        if (value.length > 0 && this.isEntity(value[0])) {
          continue;
        } else {
          this.addColumnType(columnTypes, columnName + "_json", "TEXT");
          rowData[columnName + "_json"] = JSON.stringify(value);
        }
      } else if (value && typeof value === "object") {
        if (this.isEntity(value)) {
          const foreignKeyColumn = columnName + "_id";
          this.addColumnType(columnTypes, foreignKeyColumn, "INTEGER");
          rowData[foreignKeyColumn] = value.id || null;
        } else {
          if (this.hasScalarFields(value)) {
            for (const [subKey, subValue] of Object.entries(value)) {
              if (!Array.isArray(subValue) && typeof subValue !== "object") {
                const prefixedColumn = columnName + "_" + this.sanitizeColumnName(subKey);
                this.addColumnType(columnTypes, prefixedColumn, this.getSQLiteType(subValue));
                rowData[prefixedColumn] = typeof subValue === "boolean" ? subValue ? 1 : 0 : subValue;
              }
            }
          } else {
            this.addColumnType(columnTypes, columnName + "_json", "TEXT");
            rowData[columnName + "_json"] = JSON.stringify(value);
          }
        }
      } else {
        this.addColumnType(columnTypes, columnName, this.getSQLiteType(value));
        rowData[columnName] = typeof value === "boolean" ? value ? 1 : 0 : value;
      }
    }
    return rowData;
  }
  hasScalarFields(obj) {
    if (!obj || typeof obj !== "object") return false;
    return Object.values(obj).some(
      (value) => typeof value !== "object" || value === null
    );
  }
  createJunctionTableSchemas(schemas) {
    const junctionTables = /* @__PURE__ */ new Set();
    for (const [fromTable, relatedTables] of this.entityRelationships.entries()) {
      for (const toTable of relatedTables) {
        const junctionName = [fromTable, toTable].sort().join("_");
        if (!junctionTables.has(junctionName)) {
          junctionTables.add(junctionName);
          schemas[junctionName] = {
            columns: {
              id: "INTEGER PRIMARY KEY AUTOINCREMENT",
              [`${fromTable}_id`]: "INTEGER",
              [`${toTable}_id`]: "INTEGER"
            },
            sample_data: []
          };
        }
      }
    }
  }
  createSchemaFromPrimitiveOrSimpleArray(data, tableName) {
    const columnTypes = {};
    const sampleData = [];
    if (Array.isArray(data)) {
      data.slice(0, 3).forEach((item) => {
        const row = this.extractSimpleFields(item, columnTypes);
        sampleData.push(row);
      });
      if (data.length > 3) {
        data.slice(3).forEach((item) => this.extractSimpleFields(item, columnTypes));
      }
    } else {
      const row = this.extractSimpleFields(data, columnTypes);
      sampleData.push(row);
    }
    const columns = this.resolveColumnTypes(columnTypes);
    if (!Object.keys(columns).includes("id") && !Object.keys(columns).includes("value")) {
      const colNames = Object.keys(columns);
      if (colNames.length === 1 && colNames[0] !== "value") {
        columns["value"] = columns[colNames[0]];
        delete columns[colNames[0]];
        sampleData.forEach((s) => {
          s["value"] = s[colNames[0]];
          delete s[colNames[0]];
        });
      }
    }
    if (Object.keys(columns).length === 0 && data === null) {
      columns["value"] = "TEXT";
    }
    return { columns, sample_data: sampleData };
  }
  createSchemaFromObject(obj, tableName) {
    const columnTypes = {};
    const rowData = this.extractSimpleFields(obj, columnTypes);
    const columns = this.resolveColumnTypes(columnTypes);
    return { columns, sample_data: [rowData] };
  }
  extractSimpleFields(obj, columnTypes) {
    const rowData = {};
    if (obj === null || typeof obj !== "object") {
      this.addColumnType(columnTypes, "value", this.getSQLiteType(obj));
      return { value: obj };
    }
    if (Array.isArray(obj)) {
      this.addColumnType(columnTypes, "array_data_json", "TEXT");
      return { array_data_json: JSON.stringify(obj) };
    }
    for (const [key, value] of Object.entries(obj)) {
      const columnName = this.sanitizeColumnName(key);
      if (value === null || typeof value !== "object") {
        this.addColumnType(columnTypes, columnName, this.getSQLiteType(value));
        rowData[columnName] = typeof value === "boolean" ? value ? 1 : 0 : value;
      } else {
        this.addColumnType(columnTypes, columnName + "_json", "TEXT");
        rowData[columnName + "_json"] = JSON.stringify(value);
      }
    }
    return rowData;
  }
  addColumnType(columnTypes, column, type) {
    if (!columnTypes[column]) columnTypes[column] = /* @__PURE__ */ new Set();
    columnTypes[column].add(type);
  }
  resolveColumnTypes(columnTypes) {
    const columns = {};
    for (const [columnName, types] of Object.entries(columnTypes)) {
      if (types.size === 1) {
        columns[columnName] = Array.from(types)[0];
      } else {
        columns[columnName] = types.has("TEXT") ? "TEXT" : types.has("REAL") ? "REAL" : "INTEGER";
      }
    }
    return columns;
  }
  ensureIdColumn(columns) {
    if (!columns.id) {
      columns.id = "INTEGER PRIMARY KEY AUTOINCREMENT";
    } else if (columns.id === "INTEGER") {
      columns.id = "INTEGER PRIMARY KEY";
    }
  }
  getSQLiteType(value) {
    if (value === null || value === void 0) return "TEXT";
    switch (typeof value) {
      case "number":
        return Number.isInteger(value) ? "INTEGER" : "REAL";
      case "boolean":
        return "INTEGER";
      case "string":
        return "TEXT";
      default:
        return "TEXT";
    }
  }
  sanitizeTableName(name) {
    if (!name || typeof name !== "string") {
      return "table_" + Math.random().toString(36).substr(2, 9);
    }
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "").toLowerCase();
    if (/^[0-9]/.test(sanitized)) {
      sanitized = "table_" + sanitized;
    }
    if (!sanitized || sanitized.length === 0) {
      sanitized = "table_" + Math.random().toString(36).substr(2, 9);
    }
    const reservedWords = ["table", "index", "view", "column", "primary", "key", "foreign", "constraint"];
    if (reservedWords.includes(sanitized)) {
      sanitized = sanitized + "_table";
    }
    return sanitized;
  }
  sanitizeColumnName(name) {
    if (!name || typeof name !== "string") {
      return "column_" + Math.random().toString(36).substr(2, 9);
    }
    let snakeCase = name.replace(/([A-Z])/g, "_$1").toLowerCase().replace(/[^a-zA-Z0-9_]/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
    if (/^[0-9]/.test(snakeCase)) {
      snakeCase = "col_" + snakeCase;
    }
    if (!snakeCase || snakeCase.length === 0) {
      snakeCase = "column_" + Math.random().toString(36).substr(2, 9);
    }
    const openTargetsTerms = {
      "ensemblid": "ensembl_id",
      "efoid": "efo_id",
      "chemblid": "chembl_id",
      "approvedsymbol": "approved_symbol",
      "approvedname": "approved_name",
      "geneticconstraint": "genetic_constraint",
      "mechanismsofaction": "mechanisms_of_action",
      "therapeuticareas": "therapeutic_areas",
      "pharmacovigilance": "pharmacovigilance"
    };
    const result = openTargetsTerms[snakeCase] || snakeCase;
    const reservedWords = ["table", "index", "view", "column", "primary", "key", "foreign", "constraint", "order", "group", "select", "from", "where"];
    if (reservedWords.includes(result)) {
      return result + "_col";
    }
    return result;
  }
};

// src/lib/SchemaParser.ts
var SchemaParser = class {
  constructor() {
    this.extractionRules = [];
    this.relationships = [];
  }
  static {
    __name(this, "SchemaParser");
  }
  /**
   * Parse a GraphQL schema string and extract type information
   */
  static parseSchema(schemaContent) {
    const types = {};
    const relationships = [];
    const typeMatches = schemaContent.matchAll(/type\s+(\w+)(?:\s+implements\s+[\w\s&]+)?\s*\{([^}]+(?:\}[^}]*)*)\}/g);
    for (const match of typeMatches) {
      const typeName = match[1];
      const typeBody = match[2];
      if (this.shouldSkipType(typeName)) {
        continue;
      }
      const fields = this.parseFields(typeBody, typeName, relationships);
      types[typeName] = {
        name: typeName,
        kind: "OBJECT",
        fields,
        description: this.extractDescription(match[0])
      };
    }
    return { types, relationships };
  }
  /**
   * Generate chunking rules based on Open Targets schema patterns
   */
  static generateChunkingRulesFromSchema(schemaInfo) {
    const rules = [
      // Base rules that apply to all types
      { fieldName: "id", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "ID fields should never be chunked" },
      { fieldName: "ensemblId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "Ensembl ID fields should never be chunked" },
      { fieldName: "efoId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "EFO ID fields should never be chunked" },
      { fieldName: "chemblId", typeName: "*", chunkThreshold: Infinity, priority: "never", reason: "ChEMBL ID fields should never be chunked" }
    ];
    for (const [typeName, typeInfo] of Object.entries(schemaInfo.types)) {
      for (const [fieldName, fieldInfo] of Object.entries(typeInfo.fields)) {
        const rule = this.generateFieldRule(typeName, fieldName, fieldInfo);
        if (rule) {
          rules.push(rule);
        }
      }
    }
    rules.push(
      // Known large text fields from Open Targets API
      { fieldName: "description", typeName: "Target", chunkThreshold: 2048, priority: "always", reason: "Target descriptions are typically very long" },
      { fieldName: "description", typeName: "Disease", chunkThreshold: 2048, priority: "always", reason: "Disease descriptions can be extensive" },
      { fieldName: "description", typeName: "Drug", chunkThreshold: 2048, priority: "always", reason: "Drug descriptions can be extensive" },
      { fieldName: "synonyms", typeName: "*", chunkThreshold: 1024, priority: "always", reason: "Synonym arrays are often extensive" },
      // Tractability and constraint data
      { fieldName: "tractability", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Tractability data contains extensive nested information" },
      { fieldName: "geneticConstraint", typeName: "Target", chunkThreshold: 2048, priority: "size-based", reason: "Genetic constraint data can be detailed" },
      { fieldName: "safety", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Safety information can be extensive" },
      // Association and evidence data - these can be huge
      { fieldName: "associatedTargets", typeName: "*", chunkThreshold: 8192, priority: "size-based", reason: "Target association connections can be extensive" },
      { fieldName: "associatedDiseases", typeName: "*", chunkThreshold: 8192, priority: "size-based", reason: "Disease association connections can be extensive" },
      { fieldName: "evidences", typeName: "*", chunkThreshold: 8192, priority: "size-based", reason: "Evidence connections can contain many detailed evidence objects" },
      { fieldName: "studies", typeName: "*", chunkThreshold: 6144, priority: "size-based", reason: "Study connections can be extensive" },
      // Pharmacovigilance and drug data
      { fieldName: "pharmacovigilance", typeName: "Drug", chunkThreshold: 8192, priority: "size-based", reason: "Pharmacovigilance data can be very large" },
      { fieldName: "mechanismsOfAction", typeName: "Drug", chunkThreshold: 4096, priority: "size-based", reason: "Mechanism of action data can be extensive" },
      { fieldName: "indications", typeName: "Drug", chunkThreshold: 4096, priority: "size-based", reason: "Drug indications can be numerous" },
      // Ontology and classification data
      { fieldName: "ontology", typeName: "Disease", chunkThreshold: 4096, priority: "size-based", reason: "Ontology data can contain extensive hierarchical information" },
      { fieldName: "therapeuticAreas", typeName: "Disease", chunkThreshold: 2048, priority: "size-based", reason: "Therapeutic area data can be extensive" },
      // Expression and interaction data
      { fieldName: "expressions", typeName: "Target", chunkThreshold: 6144, priority: "size-based", reason: "Expression data can be extensive across tissues" },
      { fieldName: "interactions", typeName: "Target", chunkThreshold: 6144, priority: "size-based", reason: "Interaction data can be extensive" },
      { fieldName: "pathways", typeName: "Target", chunkThreshold: 4096, priority: "size-based", reason: "Pathway data can be extensive" },
      // Conservative chunking for names and identifiers
      { fieldName: "approvedName", typeName: "*", chunkThreshold: 512, priority: "size-based", reason: "Approved names are usually short but can be long" },
      { fieldName: "approvedSymbol", typeName: "*", chunkThreshold: 256, priority: "size-based", reason: "Symbols are usually short" },
      { fieldName: "name", typeName: "*", chunkThreshold: 512, priority: "size-based", reason: "Names are usually short but can be long" }
    );
    return rules;
  }
  /**
   * Identify the most critical types for chunking optimization in Open Targets
   */
  static identifyHighValueTypes(schemaInfo) {
    const highValueTypes = [];
    const coreTypes = ["Target", "Disease", "Drug", "Evidence", "Study", "Association"];
    for (const typeName of coreTypes) {
      const typeInfo = schemaInfo.types[typeName];
      if (typeInfo) {
        const largeFields = Object.keys(typeInfo.fields).filter(
          (fieldName) => this.isLikelyLargeField(fieldName, typeInfo.fields[fieldName])
        );
        highValueTypes.push({
          typeName,
          reason: `Core Open Targets entity with ${largeFields.length} potentially large fields`,
          largeFields,
          estimatedSize: this.estimateTypeSize(typeInfo)
        });
      }
    }
    return highValueTypes;
  }
  // Private helper methods
  static shouldSkipType(typeName) {
    const skipPatterns = [
      /^__/,
      // Introspection types
      /Input$/,
      // Input types
      /Payload$/,
      // Mutation payloads
      /Connection$/,
      // GraphQL connections
      /Edge$/,
      // GraphQL edges
      /^(String|Int|Float|Boolean|ID)$/
      // Scalars
    ];
    return skipPatterns.some((pattern) => pattern.test(typeName));
  }
  static parseFields(typeBody, typeName, relationships) {
    const fields = {};
    const fieldMatches = typeBody.matchAll(/^\s*([a-zA-Z]\w*)\s*(?:\([^)]*\))?\s*:\s*([^!\n]+[!]?)/gm);
    for (const match of fieldMatches) {
      const fieldName = match[1];
      const fieldType = match[2].trim();
      if (fieldName.includes('"""') || fieldType.includes('"""')) {
        continue;
      }
      const fieldInfo = this.parseFieldType(fieldType);
      fields[fieldName] = {
        name: fieldName,
        ...fieldInfo
      };
      if (this.isRelationshipField(fieldInfo, typeName)) {
        relationships.push({
          fromType: typeName,
          toType: this.extractRelatedType(fieldInfo.type),
          fieldName,
          cardinality: fieldInfo.isList ? "one-to-many" : "one-to-one"
        });
      }
    }
    return fields;
  }
  static parseFieldType(typeString) {
    let type = typeString.trim();
    let isList = false;
    let isNullable = true;
    if (type.startsWith("[") && type.endsWith("]")) {
      isList = true;
      type = type.slice(1, -1);
    }
    if (type.endsWith("!")) {
      isNullable = false;
      type = type.slice(0, -1);
    }
    if (isList && type.endsWith("!")) {
      type = type.slice(0, -1);
    }
    return {
      type: type.trim(),
      isList,
      isNullable
    };
  }
  static isRelationshipField(fieldInfo, typeName) {
    const scalarTypes = ["String", "Int", "Float", "Boolean", "ID", "JSON", "ISO8601DateTime"];
    if (scalarTypes.includes(fieldInfo.type)) {
      return false;
    }
    const enumPatterns = [/Level$/, /Type$/, /Status$/, /Direction$/, /Category$/];
    if (enumPatterns.some((pattern) => pattern.test(fieldInfo.type))) {
      return false;
    }
    return true;
  }
  static extractRelatedType(typeString) {
    return typeString.replace(/[[\]!]/g, "");
  }
  static extractDescription(typeDefinition) {
    const descMatch = typeDefinition.match(/"""([^"]+)"""/);
    return descMatch ? descMatch[1].trim() : void 0;
  }
  static generateFieldRule(typeName, fieldName, fieldInfo) {
    if (this.isLikelyLargeField(fieldName, fieldInfo)) {
      if (fieldInfo.type === "String") {
        const textFieldThresholds = {
          "description": 2048,
          "summary": 1024,
          "synonyms": 1024,
          "approvedName": 512,
          "name": 512
        };
        const threshold = textFieldThresholds[fieldName] || 512;
        return {
          fieldName,
          typeName,
          chunkThreshold: threshold,
          priority: "size-based",
          reason: `String field '${fieldName}' on type '${typeName}' likely contains large text content`
        };
      } else if (fieldInfo.type === "JSON") {
        return {
          fieldName,
          typeName,
          chunkThreshold: 4096,
          priority: "size-based",
          reason: `JSON field '${fieldName}' on type '${typeName}' can contain large structured data`
        };
      } else if (fieldInfo.isList) {
        return {
          fieldName,
          typeName,
          chunkThreshold: 8192,
          priority: "size-based",
          reason: `List field '${fieldName}' on type '${typeName}' can contain many items`
        };
      }
    }
    return null;
  }
  static isLikelyLargeField(fieldName, fieldInfo) {
    const largeContentIndicators = [
      "description",
      "summary",
      "synonyms",
      "evidence",
      "associations",
      "tractability",
      "constraint",
      "safety",
      "pharmacovigilance",
      "mechanisms",
      "indications",
      "ontology",
      "expressions",
      "interactions",
      "pathways",
      "therapeuticAreas",
      "studies",
      "targets",
      "diseases"
    ];
    if (largeContentIndicators.some(
      (indicator) => fieldName.toLowerCase().includes(indicator)
    )) {
      return true;
    }
    if (fieldInfo.type === "JSON") {
      return true;
    }
    if (fieldInfo.type.includes("Connection")) {
      return true;
    }
    if (fieldInfo.isList && !fieldName.includes("Id")) {
      return true;
    }
    return false;
  }
  static estimateTypeSize(typeInfo) {
    const fieldCount = Object.keys(typeInfo.fields).length;
    const largeFieldCount = Object.entries(typeInfo.fields).filter(
      ([name, field]) => this.isLikelyLargeField(name, field)
    ).length;
    if (largeFieldCount >= 5 || fieldCount >= 50) {
      return "very_large";
    } else if (largeFieldCount >= 3 || fieldCount >= 30) {
      return "large";
    } else if (largeFieldCount >= 1 || fieldCount >= 15) {
      return "medium";
    } else {
      return "small";
    }
  }
  /**
   * Parse the GraphQL schema file and extract structure information
   */
  async parseSchemaFromFile(schemaPath) {
    throw new Error("File system access not available in Workers environment. Use parseSchemaContent() instead.");
  }
  /**
   * Parse GraphQL schema content and extract type/relationship information
   */
  parseSchemaContent(schemaContent) {
    const types = {};
    const relationships = [];
    const typeBlocks = this.extractTypeBlocks(schemaContent);
    for (const block of typeBlocks) {
      const typeInfo = this.parseTypeBlock(block);
      if (typeInfo) {
        types[typeInfo.name] = typeInfo;
        const typeRelationships = this.extractRelationshipsFromType(typeInfo);
        relationships.push(...typeRelationships);
      }
    }
    this.schemaInfo = { types, relationships };
    this.generateExtractionRules();
    return this.schemaInfo;
  }
  /**
   * Get extraction rules for intelligent entity processing
   */
  getExtractionRules() {
    return this.extractionRules;
  }
  /**
   * Get relationship information
   */
  getRelationships() {
    return this.relationships;
  }
  /**
   * Check if a field should have its entities extracted vs stored as JSON
   */
  shouldExtractEntities(typeName, fieldName) {
    const rule = this.extractionRules.find(
      (r) => (r.typeName === typeName || r.typeName === "*") && r.fieldName === fieldName
    );
    if (rule) {
      return {
        extract: rule.shouldExtractEntities,
        targetType: rule.targetEntityType,
        isListField: rule.isListField
      };
    }
    const entityFieldPatterns = [
      /.*targets?$/i,
      /.*diseases?$/i,
      /.*drugs?$/i,
      /.*evidences?$/i,
      /.*studies?$/i,
      /.*associations?$/i,
      /.*variants?$/i,
      /.*genes?$/i
    ];
    const suggestsEntities = entityFieldPatterns.some((pattern) => pattern.test(fieldName));
    return {
      extract: suggestsEntities,
      targetType: this.inferTargetType(fieldName),
      isListField: fieldName.endsWith("s")
      // Simple heuristic
    };
  }
  /**
   * Extract type definition blocks from schema content
   */
  extractTypeBlocks(schemaContent) {
    const typeBlocks = [];
    const lines = schemaContent.split("\n");
    let currentBlock = "";
    let inTypeDefinition = false;
    let braceCount = 0;
    for (const line of lines) {
      const trimmedLine = line.trim();
      if (!inTypeDefinition && (trimmedLine.startsWith("#") || trimmedLine === "")) {
        continue;
      }
      if (trimmedLine.match(/^(type|interface|enum|input)\s+\w+/)) {
        if (currentBlock.trim()) {
          typeBlocks.push(currentBlock.trim());
        }
        currentBlock = line + "\n";
        inTypeDefinition = true;
        braceCount = 0;
      } else if (inTypeDefinition) {
        currentBlock += line + "\n";
        braceCount += (line.match(/\{/g) || []).length;
        braceCount -= (line.match(/\}/g) || []).length;
        if (braceCount === 0 && trimmedLine.includes("}")) {
          typeBlocks.push(currentBlock.trim());
          currentBlock = "";
          inTypeDefinition = false;
        }
      }
    }
    if (currentBlock.trim()) {
      typeBlocks.push(currentBlock.trim());
    }
    return typeBlocks;
  }
  /**
   * Parse individual type block into TypeInfo
   */
  parseTypeBlock(block) {
    const lines = block.split("\n");
    const firstLine = lines[0].trim();
    const typeMatch = firstLine.match(/^(type|interface|enum|input)\s+(\w+)/);
    if (!typeMatch) return null;
    const [, kind, name] = typeMatch;
    const fields = {};
    for (let i = 1; i < lines.length - 1; i++) {
      const line = lines[i].trim();
      if (line && !line.startsWith("#") && !line.startsWith('"""')) {
        const fieldInfo = this.parseFieldLine(line);
        if (fieldInfo) {
          fields[fieldInfo.name] = fieldInfo;
        }
      }
    }
    return {
      name,
      kind: kind.toUpperCase(),
      fields,
      description: this.extractDescription(block)
    };
  }
  /**
   * Parse individual field line
   */
  parseFieldLine(line) {
    const fieldMatch = line.match(/^(\w+)(?:\([^)]*\))?:\s*(\[?)([^!\[\]]+)(!?)\]?(!?)/);
    if (!fieldMatch) return null;
    const [, name, listStart, type, typeRequired, listRequired] = fieldMatch;
    return {
      name,
      type: type.trim(),
      isList: !!listStart,
      isNullable: !typeRequired && !listRequired,
      description: void 0
      // Could be enhanced to extract field descriptions
    };
  }
  /**
   * Extract relationships from a type definition
   */
  extractRelationshipsFromType(typeInfo) {
    const relationships = [];
    for (const [fieldName, fieldInfo] of Object.entries(typeInfo.fields)) {
      if (this.isEntityType(fieldInfo.type)) {
        const cardinality = fieldInfo.isList ? "one-to-many" : "one-to-one";
        relationships.push({
          fromType: typeInfo.name,
          toType: fieldInfo.type,
          fieldName,
          cardinality
        });
      }
    }
    return relationships;
  }
  /**
   * Check if a type name represents an entity (vs scalar)
   */
  isEntityType(typeName) {
    const scalarTypes = ["String", "Int", "Float", "Boolean", "ID", "DateTime", "ISO8601DateTime"];
    return !scalarTypes.includes(typeName) && typeName[0] === typeName[0].toUpperCase();
  }
  /**
   * Generate field extraction rules based on schema analysis
   */
  generateExtractionRules() {
    if (!this.schemaInfo) return;
    this.extractionRules = [];
    for (const [typeName, typeInfo] of Object.entries(this.schemaInfo.types)) {
      for (const [fieldName, fieldInfo] of Object.entries(typeInfo.fields)) {
        if (fieldInfo.isList && this.isEntityType(fieldInfo.type)) {
          this.extractionRules.push({
            fieldName,
            typeName,
            shouldExtractEntities: true,
            targetEntityType: fieldInfo.type,
            isListField: true
          });
        } else if (!fieldInfo.isList && this.isEntityType(fieldInfo.type)) {
          this.extractionRules.push({
            fieldName,
            typeName,
            shouldExtractEntities: true,
            targetEntityType: fieldInfo.type,
            isListField: false
          });
        } else {
          this.extractionRules.push({
            fieldName,
            typeName,
            shouldExtractEntities: false,
            isListField: fieldInfo.isList
          });
        }
      }
    }
    this.extractionRules.push(
      { fieldName: "id", typeName: "*", shouldExtractEntities: false, isListField: false },
      { fieldName: "ensemblId", typeName: "*", shouldExtractEntities: false, isListField: false },
      { fieldName: "efoId", typeName: "*", shouldExtractEntities: false, isListField: false },
      { fieldName: "chemblId", typeName: "*", shouldExtractEntities: false, isListField: false },
      { fieldName: "name", typeName: "*", shouldExtractEntities: false, isListField: false },
      { fieldName: "description", typeName: "*", shouldExtractEntities: false, isListField: false }
    );
  }
  /**
   * Infer target entity type from field name
   */
  inferTargetType(fieldName) {
    const singular = fieldName.endsWith("s") ? fieldName.slice(0, -1) : fieldName;
    return singular.charAt(0).toUpperCase() + singular.slice(1);
  }
  /**
   * Extract description from type block
   */
  extractDescription(block) {
    const descMatch = block.match(/"""([\s\S]*?)"""/);
    return descMatch ? descMatch[1].trim() : void 0;
  }
};

// src/lib/DataInsertionEngine.ts
var DataInsertionEngine = class {
  constructor() {
    this.chunkingEngine = new ChunkingEngine();
    this.schemaParser = new SchemaParser();
    this.processedEntities = /* @__PURE__ */ new Map();
    this.relationshipData = /* @__PURE__ */ new Map();
    // Track actual relationships found in data
    this.extractionRules = [];
  }
  static {
    __name(this, "DataInsertionEngine");
  }
  /**
   * Configure schema-aware entity extraction
   */
  configureSchemaAwareExtraction(schemaContent) {
    const schemaInfo = this.schemaParser.parseSchemaContent(schemaContent);
    this.extractionRules = this.schemaParser.getExtractionRules();
    this.chunkingEngine.configureSchemaAwareness(schemaInfo);
  }
  /**
   * Check if entities should be extracted from a field based on schema rules
   */
  shouldExtractEntitiesFromField(typeName, fieldName) {
    return this.schemaParser.shouldExtractEntities(typeName, fieldName);
  }
  async insertData(data, schemas, sql) {
    this.processedEntities.clear();
    this.relationshipData.clear();
    const schemaNames = Object.keys(schemas);
    if (schemaNames.length === 1 && (schemaNames[0] === "scalar_data" || schemaNames[0] === "array_data" || schemaNames[0] === "root_object")) {
      const tableName = schemaNames[0];
      const schema = schemas[tableName];
      if (tableName === "scalar_data" || tableName === "root_object") {
        await this.insertSimpleRow(data, tableName, schema, sql);
      } else {
        if (Array.isArray(data)) {
          for (const item of data) {
            await this.insertSimpleRow(item, tableName, schema, sql);
          }
        } else {
          await this.insertSimpleRow(data, tableName, schema, sql);
        }
      }
      return;
    }
    await this.insertAllEntities(data, schemas, sql);
    await this.insertJunctionTableRecords(data, schemas, sql);
  }
  async insertAllEntities(obj, schemas, sql, path = []) {
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj)) {
      for (const item of obj) {
        await this.insertAllEntities(item, schemas, sql, path);
      }
      return;
    }
    if (obj.edges && Array.isArray(obj.edges)) {
      const nodes = obj.edges.map((edge) => edge.node).filter(Boolean);
      for (const node of nodes) {
        await this.insertAllEntities(node, schemas, sql, path);
      }
      return;
    }
    if (obj.rows && Array.isArray(obj.rows)) {
      for (const row of obj.rows) {
        await this.insertAllEntities(row, schemas, sql, path);
      }
      return;
    }
    if (this.isEntity(obj)) {
      const entityType = this.inferEntityType(obj, path);
      if (schemas[entityType]) {
        await this.insertEntityRecord(obj, entityType, schemas[entityType], sql);
        await this.processEntityRelationships(obj, entityType, schemas, sql, path);
      }
    }
    for (const [key, value] of Object.entries(obj)) {
      await this.insertAllEntities(value, schemas, sql, [...path, key]);
    }
  }
  async processEntityRelationships(entity, entityType, schemas, sql, path) {
    for (const [key, value] of Object.entries(entity)) {
      if (Array.isArray(value) && value.length > 0) {
        const extractionInfo = this.shouldExtractEntitiesFromField(entityType, key);
        if (extractionInfo.extract && value.length > 0 && this.isEntity(value[0])) {
          const relatedEntityType = extractionInfo.targetType || this.inferEntityType(value[0], [key]);
          for (const item of value) {
            if (this.isEntity(item) && schemas[relatedEntityType]) {
              await this.insertEntityRecord(item, relatedEntityType, schemas[relatedEntityType], sql);
              const relationshipKey = [entityType, relatedEntityType].sort().join("_");
              const relationships = this.relationshipData.get(relationshipKey) || /* @__PURE__ */ new Set();
              const entityId = this.getEntityId(entity, entityType);
              const relatedId = this.getEntityId(item, relatedEntityType);
              if (entityId && relatedId) {
                relationships.add(`${entityId}_${relatedId}`);
                this.relationshipData.set(relationshipKey, relationships);
              }
              await this.processEntityRelationships(item, relatedEntityType, schemas, sql, [...path, key]);
            }
          }
        } else {
          const firstItem = value.find((item) => this.isEntity(item));
          if (firstItem) {
            const relatedEntityType = this.inferEntityType(firstItem, [key]);
            for (const item of value) {
              if (this.isEntity(item) && schemas[relatedEntityType]) {
                await this.insertEntityRecord(item, relatedEntityType, schemas[relatedEntityType], sql);
                const relationshipKey = [entityType, relatedEntityType].sort().join("_");
                const relationships = this.relationshipData.get(relationshipKey) || /* @__PURE__ */ new Set();
                const entityId = this.getEntityId(entity, entityType);
                const relatedId = this.getEntityId(item, relatedEntityType);
                if (entityId && relatedId) {
                  relationships.add(`${entityId}_${relatedId}`);
                  this.relationshipData.set(relationshipKey, relationships);
                }
                await this.processEntityRelationships(item, relatedEntityType, schemas, sql, [...path, key]);
              }
            }
          }
        }
      } else if (value && typeof value === "object" && this.isEntity(value)) {
        const relatedEntityType = this.inferEntityType(value, [key]);
        if (schemas[relatedEntityType]) {
          await this.insertEntityRecord(value, relatedEntityType, schemas[relatedEntityType], sql);
          await this.processEntityRelationships(value, relatedEntityType, schemas, sql, [...path, key]);
        }
      }
    }
  }
  async insertEntityRecord(entity, tableName, schema, sql) {
    const entityMap = this.processedEntities.get(tableName) || /* @__PURE__ */ new Map();
    if (entityMap.has(entity)) {
      return entityMap.get(entity);
    }
    const rowData = await this.mapEntityToSchema(entity, schema, sql);
    if (Object.keys(rowData).length === 0) return null;
    const columns = Object.keys(rowData);
    const placeholders = columns.map(() => "?").join(", ");
    const values = Object.values(rowData);
    const insertSQL = `INSERT OR IGNORE INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders})`;
    sql.exec(insertSQL, ...values);
    let insertedId = null;
    if (rowData.id) {
      insertedId = rowData.id;
    } else {
      insertedId = sql.exec(`SELECT last_insert_rowid() as id`).one()?.id || null;
    }
    if (insertedId) {
      entityMap.set(entity, insertedId);
      this.processedEntities.set(tableName, entityMap);
    }
    return insertedId;
  }
  async insertJunctionTableRecords(data, schemas, sql) {
    for (const [relationshipKey, relationshipPairs] of this.relationshipData.entries()) {
      if (schemas[relationshipKey]) {
        const [table1, table22] = relationshipKey.split("_");
        for (const pairKey of relationshipPairs) {
          const [id1, id2] = pairKey.split("_").map(Number);
          const insertSQL = `INSERT OR IGNORE INTO ${relationshipKey} (${table1}_id, ${table22}_id) VALUES (?, ?)`;
          sql.exec(insertSQL, id1, id2);
        }
      }
    }
  }
  getEntityId(entity, entityType) {
    const entityMap = this.processedEntities.get(entityType);
    return entityMap?.get(entity) || null;
  }
  async mapEntityToSchema(obj, schema, sql) {
    const rowData = {};
    if (!obj || typeof obj !== "object") {
      if (schema.columns.value) rowData.value = obj;
      return rowData;
    }
    for (const columnName of Object.keys(schema.columns)) {
      if (columnName === "id" && schema.columns[columnName].includes("AUTOINCREMENT")) {
        continue;
      }
      let value = null;
      if (columnName.endsWith("_id") && !columnName.includes("_json")) {
        const baseKey = columnName.slice(0, -3);
        const originalKey = this.findOriginalKey(obj, baseKey);
        if (originalKey && obj[originalKey] && typeof obj[originalKey] === "object") {
          value = obj[originalKey].id || null;
        }
      } else if (columnName.includes("_") && !columnName.endsWith("_json")) {
        const parts = columnName.split("_");
        if (parts.length >= 2) {
          const baseKey = parts[0];
          const subKey = parts.slice(1).join("_");
          const originalKey = this.findOriginalKey(obj, baseKey);
          if (originalKey && obj[originalKey] && typeof obj[originalKey] === "object") {
            const nestedObj = obj[originalKey];
            const originalSubKey = this.findOriginalKey(nestedObj, subKey);
            if (originalSubKey && nestedObj[originalSubKey] !== void 0) {
              value = nestedObj[originalSubKey];
              if (typeof value === "boolean") value = value ? 1 : 0;
            }
          }
        }
      } else if (columnName.endsWith("_json")) {
        const baseKey = columnName.slice(0, -5);
        const originalKey = this.findOriginalKey(obj, baseKey);
        if (originalKey && obj[originalKey] && typeof obj[originalKey] === "object") {
          value = await this.chunkingEngine.smartJsonStringify(obj[originalKey], sql);
        }
      } else {
        const originalKey = this.findOriginalKey(obj, columnName);
        if (originalKey && obj[originalKey] !== void 0) {
          value = obj[originalKey];
          if (typeof value === "boolean") value = value ? 1 : 0;
          if (Array.isArray(value) && value.length > 0 && this.isEntity(value[0])) {
            continue;
          }
        }
      }
      if (value !== null && value !== void 0) {
        rowData[columnName] = value;
      }
    }
    return rowData;
  }
  // Entity detection and type inference (adapted for Open Targets patterns)
  isEntity(obj) {
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) return false;
    const hasId = obj.id !== void 0 || obj._id !== void 0 || obj.ensemblId !== void 0 || obj.efoId !== void 0 || obj.chemblId !== void 0;
    const fieldCount = Object.keys(obj).length;
    const hasMultipleFields = fieldCount >= 2;
    const hasEntityFields = obj.name !== void 0 || obj.approvedSymbol !== void 0 || obj.description !== void 0 || obj.type !== void 0 || obj.score !== void 0;
    return hasId || hasMultipleFields && hasEntityFields;
  }
  inferEntityType(obj, path) {
    if (obj.__typename) return this.sanitizeTableName(obj.__typename);
    if (obj.type && typeof obj.type === "string") return this.sanitizeTableName(obj.type);
    if (obj.ensemblId) return "target";
    if (obj.efoId) return "disease";
    if (obj.chemblId) return "drug";
    if (obj.approvedSymbol) return "target";
    if (path.length > 0) {
      const lastPath = path[path.length - 1];
      if (lastPath === "edges" && path.length > 1) {
        return this.sanitizeTableName(path[path.length - 2]);
      }
      if (lastPath === "rows" && path.length > 1) {
        return this.sanitizeTableName(path[path.length - 2]);
      }
      if (lastPath.endsWith("s") && lastPath.length > 1) {
        return this.sanitizeTableName(lastPath.slice(0, -1));
      }
      return this.sanitizeTableName(lastPath);
    }
    return "entity_" + Math.random().toString(36).substr(2, 9);
  }
  sanitizeTableName(name) {
    if (!name || typeof name !== "string") {
      return "table_" + Math.random().toString(36).substr(2, 9);
    }
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "").toLowerCase();
    if (/^[0-9]/.test(sanitized)) {
      sanitized = "table_" + sanitized;
    }
    if (!sanitized || sanitized.length === 0) {
      sanitized = "table_" + Math.random().toString(36).substr(2, 9);
    }
    const reservedWords = ["table", "index", "view", "column", "primary", "key", "foreign", "constraint"];
    if (reservedWords.includes(sanitized)) {
      sanitized = sanitized + "_table";
    }
    return sanitized;
  }
  findOriginalKey(obj, sanitizedKey) {
    const keys = Object.keys(obj);
    if (keys.includes(sanitizedKey)) return sanitizedKey;
    return keys.find(
      (key) => this.sanitizeColumnName(key) === sanitizedKey
    ) || null;
  }
  sanitizeColumnName(name) {
    if (!name || typeof name !== "string") {
      return "column_" + Math.random().toString(36).substr(2, 9);
    }
    let snakeCase = name.replace(/([A-Z])/g, "_$1").toLowerCase().replace(/[^a-zA-Z0-9_]/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
    if (/^[0-9]/.test(snakeCase)) {
      snakeCase = "col_" + snakeCase;
    }
    if (!snakeCase || snakeCase.length === 0) {
      snakeCase = "column_" + Math.random().toString(36).substr(2, 9);
    }
    const openTargetsTerms = {
      "ensemblid": "ensembl_id",
      "efoid": "efo_id",
      "chemblid": "chembl_id",
      "approvedsymbol": "approved_symbol",
      "approvedname": "approved_name",
      "geneticconstraint": "genetic_constraint",
      "mechanismsofaction": "mechanisms_of_action",
      "therapeuticareas": "therapeutic_areas",
      "pharmacovigilance": "pharmacovigilance"
    };
    const result = openTargetsTerms[snakeCase] || snakeCase;
    const reservedWords = ["table", "index", "view", "column", "primary", "key", "foreign", "constraint", "order", "group", "select", "from", "where"];
    if (reservedWords.includes(result)) {
      return result + "_col";
    }
    return result;
  }
  async insertSimpleRow(obj, tableName, schema, sql) {
    const rowData = await this.mapObjectToSimpleSchema(obj, schema, sql);
    if (Object.keys(rowData).length === 0 && !(tableName === "scalar_data" && obj === null)) return;
    const columns = Object.keys(rowData);
    const placeholders = columns.map(() => "?").join(", ");
    const values = Object.values(rowData);
    const insertSQL = `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders})`;
    sql.exec(insertSQL, ...values);
  }
  async mapObjectToSimpleSchema(obj, schema, sql) {
    const rowData = {};
    if (obj === null || typeof obj !== "object") {
      if (schema.columns.value) {
        rowData.value = obj;
      } else if (Object.keys(schema.columns).length > 0) {
        const firstCol = Object.keys(schema.columns)[0];
        rowData[firstCol] = obj;
      }
      return rowData;
    }
    if (Array.isArray(obj)) {
    }
    for (const columnName of Object.keys(schema.columns)) {
      let valueToInsert = void 0;
      let originalKeyFound = false;
      if (columnName.endsWith("_json")) {
        const baseKey = columnName.slice(0, -5);
        const originalKey = this.findOriginalKey(obj, baseKey);
        if (originalKey && obj[originalKey] !== void 0) {
          valueToInsert = await this.chunkingEngine.smartJsonStringify(obj[originalKey], sql);
          originalKeyFound = true;
        }
      } else {
        const originalKey = this.findOriginalKey(obj, columnName);
        if (originalKey && obj[originalKey] !== void 0) {
          const val = obj[originalKey];
          if (typeof val === "boolean") {
            valueToInsert = val ? 1 : 0;
          } else if (typeof val === "object" && val !== null) {
            valueToInsert = await this.chunkingEngine.smartJsonStringify(val, sql);
          } else {
            valueToInsert = val;
          }
          originalKeyFound = true;
        }
      }
      if (originalKeyFound && valueToInsert !== void 0) {
        rowData[columnName] = valueToInsert;
      } else if (obj.hasOwnProperty(columnName) && obj[columnName] !== void 0) {
        const val = obj[columnName];
        if (typeof val === "boolean") valueToInsert = val ? 1 : 0;
        else if (typeof val === "object" && val !== null) valueToInsert = await this.chunkingEngine.smartJsonStringify(val, sql);
        else valueToInsert = val;
        rowData[columnName] = valueToInsert;
      }
    }
    return rowData;
  }
};

// src/lib/PaginationAnalyzer.ts
var PaginationAnalyzer = class {
  static {
    __name(this, "PaginationAnalyzer");
  }
  static extractInfo(data) {
    const result = {
      hasNextPage: false,
      hasPreviousPage: false,
      currentCount: 0,
      totalCount: null,
      endCursor: null,
      startCursor: null
    };
    const pageInfo = this.findPageInfo(data);
    if (pageInfo) {
      Object.assign(result, {
        hasNextPage: pageInfo.hasNextPage || false,
        hasPreviousPage: pageInfo.hasPreviousPage || false,
        endCursor: pageInfo.endCursor,
        startCursor: pageInfo.startCursor
      });
    }
    result.totalCount = this.findTotalCount(data);
    result.currentCount = this.countCurrentItems(data);
    if (result.hasNextPage) {
      result.suggestion = `Use pagination to get more than ${result.currentCount} records. Add "pageInfo { hasNextPage endCursor }" to your query and use "after: "${result.endCursor}"" for next page.`;
    }
    return result;
  }
  static findPageInfo(obj) {
    if (!obj || typeof obj !== "object") return null;
    if (obj.pageInfo && typeof obj.pageInfo === "object") return obj.pageInfo;
    for (const value of Object.values(obj)) {
      const found = this.findPageInfo(value);
      if (found) return found;
    }
    return null;
  }
  static findTotalCount(obj) {
    if (!obj || typeof obj !== "object") return null;
    if (typeof obj.totalCount === "number") return obj.totalCount;
    for (const value of Object.values(obj)) {
      const found = this.findTotalCount(value);
      if (found !== null) return found;
    }
    return null;
  }
  static countCurrentItems(obj) {
    const edgesArrays = [];
    this.findEdgesArrays(obj, edgesArrays);
    if (edgesArrays.length > 0) {
      return edgesArrays.reduce((sum, edges) => sum + edges.length, 0);
    }
    return this.countArrayItems(obj);
  }
  static findEdgesArrays(obj, result) {
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj.edges)) result.push(obj.edges);
    for (const value of Object.values(obj)) {
      this.findEdgesArrays(value, result);
    }
  }
  static countArrayItems(obj) {
    if (!obj || typeof obj !== "object") return 0;
    let count3 = 0;
    for (const value of Object.values(obj)) {
      if (Array.isArray(value)) {
        count3 += value.length;
      } else if (typeof value === "object") {
        count3 += this.countArrayItems(value);
      }
    }
    return count3;
  }
};

// src/do.ts
var JsonToSqlDO = class extends DurableObject {
  constructor(ctx, env2) {
    super(ctx, env2);
    this.chunkingEngine = new ChunkingEngine();
  }
  static {
    __name(this, "JsonToSqlDO");
  }
  async processAndStoreJson(jsonData) {
    try {
      let dataToProcess = jsonData?.data ? jsonData.data : jsonData;
      const paginationInfo = PaginationAnalyzer.extractInfo(dataToProcess);
      const schemaEngine = new SchemaInferenceEngine();
      const schemas = schemaEngine.inferFromJSON(dataToProcess);
      await this.createTables(schemas);
      const dataInsertionEngine = new DataInsertionEngine();
      await dataInsertionEngine.insertData(dataToProcess, schemas, this.ctx.storage.sql);
      const metadata = await this.generateMetadata(schemas);
      if (paginationInfo.hasNextPage) {
        metadata.pagination = paginationInfo;
      }
      return {
        success: true,
        message: "Open Targets data processed successfully",
        ...metadata
      };
    } catch (error3) {
      return {
        success: false,
        message: error3 instanceof Error ? error3.message : "Processing failed"
      };
    }
  }
  async executeSql(sqlQuery) {
    try {
      const validationResult = this.validateAnalyticalSql(sqlQuery);
      if (!validationResult.isValid) {
        throw new Error(validationResult.error);
      }
      const result = this.ctx.storage.sql.exec(sqlQuery);
      const results = result.toArray();
      return {
        success: true,
        results,
        row_count: results.length,
        column_names: result.columnNames || [],
        query_type: validationResult.queryType
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "SQL execution failed",
        query: sqlQuery
      };
    }
  }
  /**
   * Enhanced SQL execution with automatic chunked content resolution
   */
  async executeEnhancedSql(sqlQuery) {
    try {
      const result = await this.executeSql(sqlQuery);
      if (!result.success) {
        return result;
      }
      const enhancedResults = await this.resolveChunkedContentInResults(result.results);
      return {
        ...result,
        results: enhancedResults,
        chunked_content_resolved: enhancedResults.length !== result.results.length || JSON.stringify(enhancedResults) !== JSON.stringify(result.results)
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "Enhanced SQL execution failed",
        query: sqlQuery
      };
    }
  }
  /**
   * Resolves chunked content references in SQL results
   */
  async resolveChunkedContentInResults(results) {
    const resolvedResults = [];
    for (const row of results) {
      const resolvedRow = {};
      for (const [key, value] of Object.entries(row)) {
        if (this.chunkingEngine.isContentReference(value)) {
          try {
            const contentId = this.chunkingEngine.extractContentId(value);
            const resolvedContent = await this.chunkingEngine.retrieveChunkedContent(
              contentId,
              this.ctx.storage.sql
            );
            if (resolvedContent !== null) {
              try {
                resolvedRow[key] = JSON.parse(resolvedContent);
              } catch {
                resolvedRow[key] = resolvedContent;
              }
            } else {
              resolvedRow[key] = `[CHUNKED_CONTENT_NOT_FOUND:${contentId}]`;
            }
          } catch (error3) {
            console.error(`Failed to resolve chunked content for ${key}:`, error3);
            resolvedRow[key] = `[CHUNKED_CONTENT_ERROR:${error3}]`;
          }
        } else {
          resolvedRow[key] = value;
        }
      }
      resolvedResults.push(resolvedRow);
    }
    return resolvedResults;
  }
  /**
   * Initialize schema-aware chunking from Open Targets GraphQL schema content
   */
  async initializeSchemaAwareChunking(schemaContent) {
    try {
      const schemaParser = new SchemaParser();
      const schemaInfo = schemaParser.parseSchemaContent(schemaContent);
      this.chunkingEngine.configureSchemaAwareness(schemaInfo);
      const extractionRules = schemaParser.getExtractionRules();
      const relationships = schemaParser.getRelationships();
      return {
        success: true,
        message: "Open Targets schema-aware chunking initialized successfully",
        schema_analysis: {
          total_types: Object.keys(schemaInfo.types).length,
          relationships_count: schemaInfo.relationships.length,
          extraction_rules_generated: extractionRules.length,
          entity_relationships: relationships.length
        },
        recommendations: [
          "Schema-aware chunking is now active for Open Targets data patterns",
          "Large content fields (tractability, associations, etc.) will be automatically detected and chunked",
          "Use the /chunking-analysis endpoint to monitor effectiveness",
          "Consider testing with real Open Targets queries to validate chunking decisions"
        ]
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "Schema initialization failed",
        suggestion: "Ensure the schema content is valid Open Targets GraphQL schema definition"
      };
    }
  }
  validateAnalyticalSql(sql) {
    const trimmedSql = sql.trim().toLowerCase();
    const allowedStarters = [
      "select",
      "with",
      // CTEs for complex analysis
      "pragma",
      // Schema inspection
      "explain",
      // Query planning
      "create temporary table",
      "create temp table",
      "create view",
      "create temporary view",
      "create temp view",
      "drop view",
      // Clean up session views
      "drop temporary table",
      "drop temp table"
    ];
    const blockedPatterns = [
      /\bdrop\s+table\s+(?!temp|temporary)/i,
      // Block permanent table drops
      /\bdelete\s+from/i,
      // Block data deletion
      /\bupdate\s+\w+\s+set/i,
      // Block data updates
      /\binsert\s+into\s+(?!temp|temporary)/i,
      // Block permanent inserts
      /\balter\s+table/i,
      // Block schema changes
      /\bcreate\s+table\s+(?!temp|temporary)/i,
      // Block permanent table creation
      /\battach\s+database/i,
      // Block external database access
      /\bdetach\s+database/i
      // Block database detachment
    ];
    const startsWithAllowed = allowedStarters.some(
      (starter) => trimmedSql.startsWith(starter)
    );
    if (!startsWithAllowed) {
      return {
        isValid: false,
        error: `Query type not allowed. Permitted operations: ${allowedStarters.join(", ")}`
      };
    }
    for (const pattern of blockedPatterns) {
      if (pattern.test(sql)) {
        return {
          isValid: false,
          error: `Operation blocked for security: ${pattern.source}`
        };
      }
    }
    let queryType = "select";
    if (trimmedSql.startsWith("with")) queryType = "cte";
    else if (trimmedSql.startsWith("pragma")) queryType = "pragma";
    else if (trimmedSql.startsWith("explain")) queryType = "explain";
    else if (trimmedSql.includes("create")) queryType = "create_temp";
    return { isValid: true, queryType };
  }
  async createTables(schemas) {
    for (const [tableName, schema] of Object.entries(schemas)) {
      try {
        const validTableName = this.validateAndFixIdentifier(tableName, "table");
        const validColumnDefs = [];
        for (const [name, type] of Object.entries(schema.columns)) {
          const validColumnName = this.validateAndFixIdentifier(name, "column");
          const validType = this.validateSQLiteType(type);
          validColumnDefs.push(`${validColumnName} ${validType}`);
        }
        if (validColumnDefs.length === 0) {
          console.warn(`Skipping table ${tableName} - no valid columns`);
          continue;
        }
        const createTableSQL = `CREATE TABLE IF NOT EXISTS ${validTableName} (${validColumnDefs.join(", ")})`;
        console.log(`Creating table with SQL: ${createTableSQL}`);
        this.ctx.storage.sql.exec(createTableSQL);
      } catch (error3) {
        console.error(`Error creating table ${tableName}:`, error3);
        try {
          const fallbackTableName = this.validateAndFixIdentifier(tableName, "table");
          const fallbackSQL = `CREATE TABLE IF NOT EXISTS ${fallbackTableName} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_json TEXT)`;
          this.ctx.storage.sql.exec(fallbackSQL);
        } catch (fallbackError) {
          console.error(`Failed to create fallback table for ${tableName}:`, fallbackError);
        }
      }
    }
  }
  validateAndFixIdentifier(name, type) {
    if (!name || typeof name !== "string") {
      return type === "table" ? "fallback_table" : "fallback_column";
    }
    let fixed = name.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
    if (/^[0-9]/.test(fixed)) {
      fixed = (type === "table" ? "table_" : "col_") + fixed;
    }
    if (!fixed || fixed.length === 0) {
      fixed = type === "table" ? "fallback_table" : "fallback_column";
    }
    const reservedWords = [
      "table",
      "index",
      "view",
      "column",
      "primary",
      "key",
      "foreign",
      "constraint",
      "order",
      "group",
      "select",
      "from",
      "where",
      "insert",
      "update",
      "delete",
      "create",
      "drop",
      "alter",
      "join",
      "inner",
      "outer",
      "left",
      "right",
      "union",
      "all",
      "distinct",
      "having",
      "limit",
      "offset",
      "as",
      "on"
    ];
    if (reservedWords.includes(fixed.toLowerCase())) {
      fixed = fixed + (type === "table" ? "_tbl" : "_col");
    }
    return fixed.toLowerCase();
  }
  validateSQLiteType(type) {
    if (!type || typeof type !== "string") {
      return "TEXT";
    }
    const upperType = type.toUpperCase();
    const validTypes = [
      "INTEGER",
      "REAL",
      "TEXT",
      "BLOB",
      "NUMERIC",
      "INTEGER PRIMARY KEY",
      "INTEGER PRIMARY KEY AUTOINCREMENT",
      "JSON"
      // SQLite supports JSON since 3.38
    ];
    if (validTypes.some((validType) => upperType.includes(validType))) {
      return type;
    }
    const typeMap = {
      "STRING": "TEXT",
      "VARCHAR": "TEXT",
      "CHAR": "TEXT",
      "CLOB": "TEXT",
      "INT": "INTEGER",
      "BIGINT": "INTEGER",
      "SMALLINT": "INTEGER",
      "TINYINT": "INTEGER",
      "FLOAT": "REAL",
      "DOUBLE": "REAL",
      "DECIMAL": "NUMERIC",
      "BOOLEAN": "INTEGER",
      "BOOL": "INTEGER",
      "DATE": "TEXT",
      "DATETIME": "TEXT",
      "TIMESTAMP": "TEXT"
    };
    return typeMap[upperType] || "TEXT";
  }
  async generateMetadata(schemas) {
    const metadata = {
      schemas: {},
      table_count: Object.keys(schemas).length,
      total_rows: 0
    };
    for (const [tableName, schema] of Object.entries(schemas)) {
      try {
        const countResult = this.ctx.storage.sql.exec(`SELECT COUNT(*) as count FROM ${tableName}`);
        const countRow = countResult.one();
        const rowCount = typeof countRow?.count === "number" ? countRow.count : 0;
        const sampleResult = this.ctx.storage.sql.exec(`SELECT * FROM ${tableName} LIMIT 3`);
        const sampleData = sampleResult.toArray();
        metadata.schemas[tableName] = {
          columns: schema.columns,
          row_count: rowCount,
          sample_data: sampleData
        };
        metadata.total_rows += rowCount;
      } catch (error3) {
        continue;
      }
    }
    return metadata;
  }
  async getSchemaInfo() {
    try {
      const tables = this.ctx.storage.sql.exec(`
				SELECT name, type 
				FROM sqlite_master 
				WHERE type IN ('table', 'view') 
				ORDER BY name
			`).toArray();
      const schemaInfo = {
        database_summary: {
          total_tables: tables.length,
          table_names: tables.map((t) => String(t.name))
        },
        tables: {}
      };
      for (const table3 of tables) {
        const tableName = String(table3.name);
        if (!tableName || tableName === "undefined" || tableName === "null") {
          continue;
        }
        try {
          const columns = this.ctx.storage.sql.exec(`PRAGMA table_info(${tableName})`).toArray();
          const countResult = this.ctx.storage.sql.exec(`SELECT COUNT(*) as count FROM ${tableName}`).one();
          const rowCount = typeof countResult?.count === "number" ? countResult.count : 0;
          const sampleData = this.ctx.storage.sql.exec(`SELECT * FROM ${tableName} LIMIT 3`).toArray();
          const foreignKeys = this.ctx.storage.sql.exec(`PRAGMA foreign_key_list(${tableName})`).toArray();
          const indexes = this.ctx.storage.sql.exec(`PRAGMA index_list(${tableName})`).toArray();
          schemaInfo.tables[tableName] = {
            type: String(table3.type),
            row_count: rowCount,
            columns: columns.map((col) => ({
              name: String(col.name),
              type: String(col.type),
              not_null: Boolean(col.notnull),
              default_value: col.dflt_value,
              primary_key: Boolean(col.pk)
            })),
            foreign_keys: foreignKeys.map((fk) => ({
              column: String(fk.from),
              references_table: String(fk.table),
              references_column: String(fk.to)
            })),
            indexes: indexes.map((idx) => ({
              name: String(idx.name),
              unique: Boolean(idx.unique)
            })),
            sample_data: sampleData
          };
        } catch (tableError) {
          console.error(`Error processing table ${tableName}:`, tableError);
          continue;
        }
      }
      return {
        success: true,
        schema_info: schemaInfo
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "Schema inspection failed"
      };
    }
  }
  async getTableColumns(tableName) {
    try {
      const columns = this.ctx.storage.sql.exec(`PRAGMA table_info(${tableName})`).toArray();
      const foreignKeys = this.ctx.storage.sql.exec(`PRAGMA foreign_key_list(${tableName})`).toArray();
      return {
        success: true,
        table: tableName,
        columns: columns.map((col) => {
          const fkRef = foreignKeys.find((fk) => fk.from === col.name);
          return {
            name: col.name,
            type: col.type,
            not_null: Boolean(col.notnull),
            default_value: col.dflt_value,
            primary_key: Boolean(col.pk),
            is_foreign_key: Boolean(fkRef),
            references: fkRef ? {
              table: fkRef.table,
              column: fkRef.to
            } : null
          };
        })
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "Table inspection failed"
      };
    }
  }
  async generateAnalyticalQueries(tableName) {
    try {
      const suggestions = {
        schema_discovery: [
          "PRAGMA table_list",
          "SELECT name FROM sqlite_master WHERE type='table'",
          tableName ? `PRAGMA table_info(${tableName})` : "-- Specify table name for column info"
        ],
        json_analysis: [
          "-- SQLite JSON functions for analyzing Open Targets data:",
          "SELECT json_extract(column_name, '$.field') FROM table_name",
          "SELECT json_array_length(column_name) FROM table_name WHERE column_name IS NOT NULL",
          "SELECT json_each.value FROM table_name, json_each(table_name.column_name)"
        ],
        statistical_analysis: [
          "-- Basic statistics:",
          "SELECT COUNT(*), AVG(numeric_column), MIN(numeric_column), MAX(numeric_column) FROM table_name",
          "-- Distribution analysis:",
          "SELECT column_name, COUNT(*) as frequency FROM table_name GROUP BY column_name ORDER BY frequency DESC",
          "-- Cross-table analysis with CTEs:",
          "WITH summary AS (SELECT ...) SELECT * FROM summary WHERE ..."
        ],
        open_targets_specific: [
          "-- Target-disease associations by score:",
          "SELECT t.approved_symbol, d.name, a.score FROM target t JOIN association a ON t.id = a.target_id JOIN disease d ON a.disease_id = d.id ORDER BY a.score DESC",
          "-- Top targets by tractability:",
          "SELECT approved_symbol, name, json_extract(tractability_json, '$.score') as tractability_score FROM target WHERE tractability_json IS NOT NULL",
          "-- Drug mechanisms of action:",
          "SELECT name, json_extract(mechanisms_of_action_json, '$[*].mechanismOfAction') FROM drug WHERE mechanisms_of_action_json IS NOT NULL",
          "-- Disease therapeutic areas:",
          "SELECT name, json_extract(therapeutic_areas_json, '$[*].name') FROM disease WHERE therapeutic_areas_json IS NOT NULL"
        ]
      };
      return {
        success: true,
        query_suggestions: suggestions
      };
    } catch (error3) {
      return {
        success: false,
        error: error3 instanceof Error ? error3.message : "Query generation failed"
      };
    }
  }
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname === "/process" && request.method === "POST") {
        const jsonData = await request.json();
        const result = await this.processAndStoreJson(jsonData);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/query" && request.method === "POST") {
        const { sql } = await request.json();
        const result = await this.executeSql(sql);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/schema" && request.method === "GET") {
        const result = await this.getSchemaInfo();
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/table-info" && request.method === "POST") {
        const { table_name } = await request.json();
        const result = await this.getTableColumns(table_name);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/query-suggestions" && request.method === "GET") {
        const tableName = url.searchParams.get("table");
        const result = await this.generateAnalyticalQueries(tableName || void 0);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/query-enhanced" && request.method === "POST") {
        const { sql } = await request.json();
        const result = await this.executeEnhancedSql(sql);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/chunking-stats" && request.method === "GET") {
        const result = await this.chunkingEngine.getChunkingStats(this.ctx.storage.sql);
        return new Response(JSON.stringify({
          success: true,
          chunking_statistics: result
        }), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/initialize-schema" && request.method === "POST") {
        const { schemaContent } = await request.json();
        const result = await this.initializeSchemaAwareChunking(schemaContent);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/chunking-analysis" && request.method === "GET") {
        const result = await this.chunkingEngine.analyzeChunkingEffectiveness(this.ctx.storage.sql);
        return new Response(JSON.stringify({
          success: true,
          analysis: result
        }), {
          headers: { "Content-Type": "application/json" }
        });
      } else if (url.pathname === "/delete" && request.method === "DELETE") {
        await this.ctx.storage.deleteAll();
        return new Response(JSON.stringify({ success: true }), {
          headers: { "Content-Type": "application/json" }
        });
      } else {
        return new Response("Not Found", { status: 404 });
      }
    } catch (error3) {
      return new Response(JSON.stringify({
        error: error3 instanceof Error ? error3.message : "Unknown error"
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      });
    }
  }
};

// node_modules/zod/lib/index.mjs
var util;
(function(util2) {
  util2.assertEqual = (val) => val;
  function assertIs(_arg) {
  }
  __name(assertIs, "assertIs");
  util2.assertIs = assertIs;
  function assertNever(_x) {
    throw new Error();
  }
  __name(assertNever, "assertNever");
  util2.assertNever = assertNever;
  util2.arrayToEnum = (items) => {
    const obj = {};
    for (const item of items) {
      obj[item] = item;
    }
    return obj;
  };
  util2.getValidEnumValues = (obj) => {
    const validKeys = util2.objectKeys(obj).filter((k) => typeof obj[obj[k]] !== "number");
    const filtered = {};
    for (const k of validKeys) {
      filtered[k] = obj[k];
    }
    return util2.objectValues(filtered);
  };
  util2.objectValues = (obj) => {
    return util2.objectKeys(obj).map(function(e) {
      return obj[e];
    });
  };
  util2.objectKeys = typeof Object.keys === "function" ? (obj) => Object.keys(obj) : (object) => {
    const keys = [];
    for (const key in object) {
      if (Object.prototype.hasOwnProperty.call(object, key)) {
        keys.push(key);
      }
    }
    return keys;
  };
  util2.find = (arr, checker) => {
    for (const item of arr) {
      if (checker(item))
        return item;
    }
    return void 0;
  };
  util2.isInteger = typeof Number.isInteger === "function" ? (val) => Number.isInteger(val) : (val) => typeof val === "number" && isFinite(val) && Math.floor(val) === val;
  function joinValues(array, separator = " | ") {
    return array.map((val) => typeof val === "string" ? `'${val}'` : val).join(separator);
  }
  __name(joinValues, "joinValues");
  util2.joinValues = joinValues;
  util2.jsonStringifyReplacer = (_, value) => {
    if (typeof value === "bigint") {
      return value.toString();
    }
    return value;
  };
})(util || (util = {}));
var objectUtil;
(function(objectUtil2) {
  objectUtil2.mergeShapes = (first, second) => {
    return {
      ...first,
      ...second
      // second overwrites first
    };
  };
})(objectUtil || (objectUtil = {}));
var ZodParsedType = util.arrayToEnum([
  "string",
  "nan",
  "number",
  "integer",
  "float",
  "boolean",
  "date",
  "bigint",
  "symbol",
  "function",
  "undefined",
  "null",
  "array",
  "object",
  "unknown",
  "promise",
  "void",
  "never",
  "map",
  "set"
]);
var getParsedType = /* @__PURE__ */ __name((data) => {
  const t = typeof data;
  switch (t) {
    case "undefined":
      return ZodParsedType.undefined;
    case "string":
      return ZodParsedType.string;
    case "number":
      return isNaN(data) ? ZodParsedType.nan : ZodParsedType.number;
    case "boolean":
      return ZodParsedType.boolean;
    case "function":
      return ZodParsedType.function;
    case "bigint":
      return ZodParsedType.bigint;
    case "symbol":
      return ZodParsedType.symbol;
    case "object":
      if (Array.isArray(data)) {
        return ZodParsedType.array;
      }
      if (data === null) {
        return ZodParsedType.null;
      }
      if (data.then && typeof data.then === "function" && data.catch && typeof data.catch === "function") {
        return ZodParsedType.promise;
      }
      if (typeof Map !== "undefined" && data instanceof Map) {
        return ZodParsedType.map;
      }
      if (typeof Set !== "undefined" && data instanceof Set) {
        return ZodParsedType.set;
      }
      if (typeof Date !== "undefined" && data instanceof Date) {
        return ZodParsedType.date;
      }
      return ZodParsedType.object;
    default:
      return ZodParsedType.unknown;
  }
}, "getParsedType");
var ZodIssueCode = util.arrayToEnum([
  "invalid_type",
  "invalid_literal",
  "custom",
  "invalid_union",
  "invalid_union_discriminator",
  "invalid_enum_value",
  "unrecognized_keys",
  "invalid_arguments",
  "invalid_return_type",
  "invalid_date",
  "invalid_string",
  "too_small",
  "too_big",
  "invalid_intersection_types",
  "not_multiple_of",
  "not_finite"
]);
var quotelessJson = /* @__PURE__ */ __name((obj) => {
  const json = JSON.stringify(obj, null, 2);
  return json.replace(/"([^"]+)":/g, "$1:");
}, "quotelessJson");
var ZodError = class _ZodError extends Error {
  static {
    __name(this, "ZodError");
  }
  get errors() {
    return this.issues;
  }
  constructor(issues) {
    super();
    this.issues = [];
    this.addIssue = (sub) => {
      this.issues = [...this.issues, sub];
    };
    this.addIssues = (subs = []) => {
      this.issues = [...this.issues, ...subs];
    };
    const actualProto = new.target.prototype;
    if (Object.setPrototypeOf) {
      Object.setPrototypeOf(this, actualProto);
    } else {
      this.__proto__ = actualProto;
    }
    this.name = "ZodError";
    this.issues = issues;
  }
  format(_mapper) {
    const mapper = _mapper || function(issue) {
      return issue.message;
    };
    const fieldErrors = { _errors: [] };
    const processError = /* @__PURE__ */ __name((error3) => {
      for (const issue of error3.issues) {
        if (issue.code === "invalid_union") {
          issue.unionErrors.map(processError);
        } else if (issue.code === "invalid_return_type") {
          processError(issue.returnTypeError);
        } else if (issue.code === "invalid_arguments") {
          processError(issue.argumentsError);
        } else if (issue.path.length === 0) {
          fieldErrors._errors.push(mapper(issue));
        } else {
          let curr = fieldErrors;
          let i = 0;
          while (i < issue.path.length) {
            const el = issue.path[i];
            const terminal = i === issue.path.length - 1;
            if (!terminal) {
              curr[el] = curr[el] || { _errors: [] };
            } else {
              curr[el] = curr[el] || { _errors: [] };
              curr[el]._errors.push(mapper(issue));
            }
            curr = curr[el];
            i++;
          }
        }
      }
    }, "processError");
    processError(this);
    return fieldErrors;
  }
  static assert(value) {
    if (!(value instanceof _ZodError)) {
      throw new Error(`Not a ZodError: ${value}`);
    }
  }
  toString() {
    return this.message;
  }
  get message() {
    return JSON.stringify(this.issues, util.jsonStringifyReplacer, 2);
  }
  get isEmpty() {
    return this.issues.length === 0;
  }
  flatten(mapper = (issue) => issue.message) {
    const fieldErrors = {};
    const formErrors = [];
    for (const sub of this.issues) {
      if (sub.path.length > 0) {
        fieldErrors[sub.path[0]] = fieldErrors[sub.path[0]] || [];
        fieldErrors[sub.path[0]].push(mapper(sub));
      } else {
        formErrors.push(mapper(sub));
      }
    }
    return { formErrors, fieldErrors };
  }
  get formErrors() {
    return this.flatten();
  }
};
ZodError.create = (issues) => {
  const error3 = new ZodError(issues);
  return error3;
};
var errorMap = /* @__PURE__ */ __name((issue, _ctx) => {
  let message;
  switch (issue.code) {
    case ZodIssueCode.invalid_type:
      if (issue.received === ZodParsedType.undefined) {
        message = "Required";
      } else {
        message = `Expected ${issue.expected}, received ${issue.received}`;
      }
      break;
    case ZodIssueCode.invalid_literal:
      message = `Invalid literal value, expected ${JSON.stringify(issue.expected, util.jsonStringifyReplacer)}`;
      break;
    case ZodIssueCode.unrecognized_keys:
      message = `Unrecognized key(s) in object: ${util.joinValues(issue.keys, ", ")}`;
      break;
    case ZodIssueCode.invalid_union:
      message = `Invalid input`;
      break;
    case ZodIssueCode.invalid_union_discriminator:
      message = `Invalid discriminator value. Expected ${util.joinValues(issue.options)}`;
      break;
    case ZodIssueCode.invalid_enum_value:
      message = `Invalid enum value. Expected ${util.joinValues(issue.options)}, received '${issue.received}'`;
      break;
    case ZodIssueCode.invalid_arguments:
      message = `Invalid function arguments`;
      break;
    case ZodIssueCode.invalid_return_type:
      message = `Invalid function return type`;
      break;
    case ZodIssueCode.invalid_date:
      message = `Invalid date`;
      break;
    case ZodIssueCode.invalid_string:
      if (typeof issue.validation === "object") {
        if ("includes" in issue.validation) {
          message = `Invalid input: must include "${issue.validation.includes}"`;
          if (typeof issue.validation.position === "number") {
            message = `${message} at one or more positions greater than or equal to ${issue.validation.position}`;
          }
        } else if ("startsWith" in issue.validation) {
          message = `Invalid input: must start with "${issue.validation.startsWith}"`;
        } else if ("endsWith" in issue.validation) {
          message = `Invalid input: must end with "${issue.validation.endsWith}"`;
        } else {
          util.assertNever(issue.validation);
        }
      } else if (issue.validation !== "regex") {
        message = `Invalid ${issue.validation}`;
      } else {
        message = "Invalid";
      }
      break;
    case ZodIssueCode.too_small:
      if (issue.type === "array")
        message = `Array must contain ${issue.exact ? "exactly" : issue.inclusive ? `at least` : `more than`} ${issue.minimum} element(s)`;
      else if (issue.type === "string")
        message = `String must contain ${issue.exact ? "exactly" : issue.inclusive ? `at least` : `over`} ${issue.minimum} character(s)`;
      else if (issue.type === "number")
        message = `Number must be ${issue.exact ? `exactly equal to ` : issue.inclusive ? `greater than or equal to ` : `greater than `}${issue.minimum}`;
      else if (issue.type === "date")
        message = `Date must be ${issue.exact ? `exactly equal to ` : issue.inclusive ? `greater than or equal to ` : `greater than `}${new Date(Number(issue.minimum))}`;
      else
        message = "Invalid input";
      break;
    case ZodIssueCode.too_big:
      if (issue.type === "array")
        message = `Array must contain ${issue.exact ? `exactly` : issue.inclusive ? `at most` : `less than`} ${issue.maximum} element(s)`;
      else if (issue.type === "string")
        message = `String must contain ${issue.exact ? `exactly` : issue.inclusive ? `at most` : `under`} ${issue.maximum} character(s)`;
      else if (issue.type === "number")
        message = `Number must be ${issue.exact ? `exactly` : issue.inclusive ? `less than or equal to` : `less than`} ${issue.maximum}`;
      else if (issue.type === "bigint")
        message = `BigInt must be ${issue.exact ? `exactly` : issue.inclusive ? `less than or equal to` : `less than`} ${issue.maximum}`;
      else if (issue.type === "date")
        message = `Date must be ${issue.exact ? `exactly` : issue.inclusive ? `smaller than or equal to` : `smaller than`} ${new Date(Number(issue.maximum))}`;
      else
        message = "Invalid input";
      break;
    case ZodIssueCode.custom:
      message = `Invalid input`;
      break;
    case ZodIssueCode.invalid_intersection_types:
      message = `Intersection results could not be merged`;
      break;
    case ZodIssueCode.not_multiple_of:
      message = `Number must be a multiple of ${issue.multipleOf}`;
      break;
    case ZodIssueCode.not_finite:
      message = "Number must be finite";
      break;
    default:
      message = _ctx.defaultError;
      util.assertNever(issue);
  }
  return { message };
}, "errorMap");
var overrideErrorMap = errorMap;
function setErrorMap(map) {
  overrideErrorMap = map;
}
__name(setErrorMap, "setErrorMap");
function getErrorMap() {
  return overrideErrorMap;
}
__name(getErrorMap, "getErrorMap");
var makeIssue = /* @__PURE__ */ __name((params) => {
  const { data, path, errorMaps, issueData } = params;
  const fullPath = [...path, ...issueData.path || []];
  const fullIssue = {
    ...issueData,
    path: fullPath
  };
  if (issueData.message !== void 0) {
    return {
      ...issueData,
      path: fullPath,
      message: issueData.message
    };
  }
  let errorMessage = "";
  const maps = errorMaps.filter((m) => !!m).slice().reverse();
  for (const map of maps) {
    errorMessage = map(fullIssue, { data, defaultError: errorMessage }).message;
  }
  return {
    ...issueData,
    path: fullPath,
    message: errorMessage
  };
}, "makeIssue");
var EMPTY_PATH = [];
function addIssueToContext(ctx, issueData) {
  const overrideMap = getErrorMap();
  const issue = makeIssue({
    issueData,
    data: ctx.data,
    path: ctx.path,
    errorMaps: [
      ctx.common.contextualErrorMap,
      // contextual error map is first priority
      ctx.schemaErrorMap,
      // then schema-bound map if available
      overrideMap,
      // then global override map
      overrideMap === errorMap ? void 0 : errorMap
      // then global default map
    ].filter((x) => !!x)
  });
  ctx.common.issues.push(issue);
}
__name(addIssueToContext, "addIssueToContext");
var ParseStatus = class _ParseStatus {
  static {
    __name(this, "ParseStatus");
  }
  constructor() {
    this.value = "valid";
  }
  dirty() {
    if (this.value === "valid")
      this.value = "dirty";
  }
  abort() {
    if (this.value !== "aborted")
      this.value = "aborted";
  }
  static mergeArray(status, results) {
    const arrayValue = [];
    for (const s of results) {
      if (s.status === "aborted")
        return INVALID;
      if (s.status === "dirty")
        status.dirty();
      arrayValue.push(s.value);
    }
    return { status: status.value, value: arrayValue };
  }
  static async mergeObjectAsync(status, pairs) {
    const syncPairs = [];
    for (const pair of pairs) {
      const key = await pair.key;
      const value = await pair.value;
      syncPairs.push({
        key,
        value
      });
    }
    return _ParseStatus.mergeObjectSync(status, syncPairs);
  }
  static mergeObjectSync(status, pairs) {
    const finalObject = {};
    for (const pair of pairs) {
      const { key, value } = pair;
      if (key.status === "aborted")
        return INVALID;
      if (value.status === "aborted")
        return INVALID;
      if (key.status === "dirty")
        status.dirty();
      if (value.status === "dirty")
        status.dirty();
      if (key.value !== "__proto__" && (typeof value.value !== "undefined" || pair.alwaysSet)) {
        finalObject[key.value] = value.value;
      }
    }
    return { status: status.value, value: finalObject };
  }
};
var INVALID = Object.freeze({
  status: "aborted"
});
var DIRTY = /* @__PURE__ */ __name((value) => ({ status: "dirty", value }), "DIRTY");
var OK = /* @__PURE__ */ __name((value) => ({ status: "valid", value }), "OK");
var isAborted = /* @__PURE__ */ __name((x) => x.status === "aborted", "isAborted");
var isDirty = /* @__PURE__ */ __name((x) => x.status === "dirty", "isDirty");
var isValid = /* @__PURE__ */ __name((x) => x.status === "valid", "isValid");
var isAsync = /* @__PURE__ */ __name((x) => typeof Promise !== "undefined" && x instanceof Promise, "isAsync");
function __classPrivateFieldGet(receiver, state, kind, f) {
  if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a getter");
  if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
  return kind === "m" ? f : kind === "a" ? f.call(receiver) : f ? f.value : state.get(receiver);
}
__name(__classPrivateFieldGet, "__classPrivateFieldGet");
function __classPrivateFieldSet(receiver, state, value, kind, f) {
  if (kind === "m") throw new TypeError("Private method is not writable");
  if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a setter");
  if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
  return kind === "a" ? f.call(receiver, value) : f ? f.value = value : state.set(receiver, value), value;
}
__name(__classPrivateFieldSet, "__classPrivateFieldSet");
var errorUtil;
(function(errorUtil2) {
  errorUtil2.errToObj = (message) => typeof message === "string" ? { message } : message || {};
  errorUtil2.toString = (message) => typeof message === "string" ? message : message === null || message === void 0 ? void 0 : message.message;
})(errorUtil || (errorUtil = {}));
var _ZodEnum_cache;
var _ZodNativeEnum_cache;
var ParseInputLazyPath = class {
  static {
    __name(this, "ParseInputLazyPath");
  }
  constructor(parent, value, path, key) {
    this._cachedPath = [];
    this.parent = parent;
    this.data = value;
    this._path = path;
    this._key = key;
  }
  get path() {
    if (!this._cachedPath.length) {
      if (this._key instanceof Array) {
        this._cachedPath.push(...this._path, ...this._key);
      } else {
        this._cachedPath.push(...this._path, this._key);
      }
    }
    return this._cachedPath;
  }
};
var handleResult = /* @__PURE__ */ __name((ctx, result) => {
  if (isValid(result)) {
    return { success: true, data: result.value };
  } else {
    if (!ctx.common.issues.length) {
      throw new Error("Validation failed but no issues detected.");
    }
    return {
      success: false,
      get error() {
        if (this._error)
          return this._error;
        const error3 = new ZodError(ctx.common.issues);
        this._error = error3;
        return this._error;
      }
    };
  }
}, "handleResult");
function processCreateParams(params) {
  if (!params)
    return {};
  const { errorMap: errorMap2, invalid_type_error, required_error, description } = params;
  if (errorMap2 && (invalid_type_error || required_error)) {
    throw new Error(`Can't use "invalid_type_error" or "required_error" in conjunction with custom error map.`);
  }
  if (errorMap2)
    return { errorMap: errorMap2, description };
  const customMap = /* @__PURE__ */ __name((iss, ctx) => {
    var _a, _b;
    const { message } = params;
    if (iss.code === "invalid_enum_value") {
      return { message: message !== null && message !== void 0 ? message : ctx.defaultError };
    }
    if (typeof ctx.data === "undefined") {
      return { message: (_a = message !== null && message !== void 0 ? message : required_error) !== null && _a !== void 0 ? _a : ctx.defaultError };
    }
    if (iss.code !== "invalid_type")
      return { message: ctx.defaultError };
    return { message: (_b = message !== null && message !== void 0 ? message : invalid_type_error) !== null && _b !== void 0 ? _b : ctx.defaultError };
  }, "customMap");
  return { errorMap: customMap, description };
}
__name(processCreateParams, "processCreateParams");
var ZodType = class {
  static {
    __name(this, "ZodType");
  }
  get description() {
    return this._def.description;
  }
  _getType(input) {
    return getParsedType(input.data);
  }
  _getOrReturnCtx(input, ctx) {
    return ctx || {
      common: input.parent.common,
      data: input.data,
      parsedType: getParsedType(input.data),
      schemaErrorMap: this._def.errorMap,
      path: input.path,
      parent: input.parent
    };
  }
  _processInputParams(input) {
    return {
      status: new ParseStatus(),
      ctx: {
        common: input.parent.common,
        data: input.data,
        parsedType: getParsedType(input.data),
        schemaErrorMap: this._def.errorMap,
        path: input.path,
        parent: input.parent
      }
    };
  }
  _parseSync(input) {
    const result = this._parse(input);
    if (isAsync(result)) {
      throw new Error("Synchronous parse encountered promise.");
    }
    return result;
  }
  _parseAsync(input) {
    const result = this._parse(input);
    return Promise.resolve(result);
  }
  parse(data, params) {
    const result = this.safeParse(data, params);
    if (result.success)
      return result.data;
    throw result.error;
  }
  safeParse(data, params) {
    var _a;
    const ctx = {
      common: {
        issues: [],
        async: (_a = params === null || params === void 0 ? void 0 : params.async) !== null && _a !== void 0 ? _a : false,
        contextualErrorMap: params === null || params === void 0 ? void 0 : params.errorMap
      },
      path: (params === null || params === void 0 ? void 0 : params.path) || [],
      schemaErrorMap: this._def.errorMap,
      parent: null,
      data,
      parsedType: getParsedType(data)
    };
    const result = this._parseSync({ data, path: ctx.path, parent: ctx });
    return handleResult(ctx, result);
  }
  "~validate"(data) {
    var _a, _b;
    const ctx = {
      common: {
        issues: [],
        async: !!this["~standard"].async
      },
      path: [],
      schemaErrorMap: this._def.errorMap,
      parent: null,
      data,
      parsedType: getParsedType(data)
    };
    if (!this["~standard"].async) {
      try {
        const result = this._parseSync({ data, path: [], parent: ctx });
        return isValid(result) ? {
          value: result.value
        } : {
          issues: ctx.common.issues
        };
      } catch (err) {
        if ((_b = (_a = err === null || err === void 0 ? void 0 : err.message) === null || _a === void 0 ? void 0 : _a.toLowerCase()) === null || _b === void 0 ? void 0 : _b.includes("encountered")) {
          this["~standard"].async = true;
        }
        ctx.common = {
          issues: [],
          async: true
        };
      }
    }
    return this._parseAsync({ data, path: [], parent: ctx }).then((result) => isValid(result) ? {
      value: result.value
    } : {
      issues: ctx.common.issues
    });
  }
  async parseAsync(data, params) {
    const result = await this.safeParseAsync(data, params);
    if (result.success)
      return result.data;
    throw result.error;
  }
  async safeParseAsync(data, params) {
    const ctx = {
      common: {
        issues: [],
        contextualErrorMap: params === null || params === void 0 ? void 0 : params.errorMap,
        async: true
      },
      path: (params === null || params === void 0 ? void 0 : params.path) || [],
      schemaErrorMap: this._def.errorMap,
      parent: null,
      data,
      parsedType: getParsedType(data)
    };
    const maybeAsyncResult = this._parse({ data, path: ctx.path, parent: ctx });
    const result = await (isAsync(maybeAsyncResult) ? maybeAsyncResult : Promise.resolve(maybeAsyncResult));
    return handleResult(ctx, result);
  }
  refine(check, message) {
    const getIssueProperties = /* @__PURE__ */ __name((val) => {
      if (typeof message === "string" || typeof message === "undefined") {
        return { message };
      } else if (typeof message === "function") {
        return message(val);
      } else {
        return message;
      }
    }, "getIssueProperties");
    return this._refinement((val, ctx) => {
      const result = check(val);
      const setError = /* @__PURE__ */ __name(() => ctx.addIssue({
        code: ZodIssueCode.custom,
        ...getIssueProperties(val)
      }), "setError");
      if (typeof Promise !== "undefined" && result instanceof Promise) {
        return result.then((data) => {
          if (!data) {
            setError();
            return false;
          } else {
            return true;
          }
        });
      }
      if (!result) {
        setError();
        return false;
      } else {
        return true;
      }
    });
  }
  refinement(check, refinementData) {
    return this._refinement((val, ctx) => {
      if (!check(val)) {
        ctx.addIssue(typeof refinementData === "function" ? refinementData(val, ctx) : refinementData);
        return false;
      } else {
        return true;
      }
    });
  }
  _refinement(refinement) {
    return new ZodEffects({
      schema: this,
      typeName: ZodFirstPartyTypeKind.ZodEffects,
      effect: { type: "refinement", refinement }
    });
  }
  superRefine(refinement) {
    return this._refinement(refinement);
  }
  constructor(def) {
    this.spa = this.safeParseAsync;
    this._def = def;
    this.parse = this.parse.bind(this);
    this.safeParse = this.safeParse.bind(this);
    this.parseAsync = this.parseAsync.bind(this);
    this.safeParseAsync = this.safeParseAsync.bind(this);
    this.spa = this.spa.bind(this);
    this.refine = this.refine.bind(this);
    this.refinement = this.refinement.bind(this);
    this.superRefine = this.superRefine.bind(this);
    this.optional = this.optional.bind(this);
    this.nullable = this.nullable.bind(this);
    this.nullish = this.nullish.bind(this);
    this.array = this.array.bind(this);
    this.promise = this.promise.bind(this);
    this.or = this.or.bind(this);
    this.and = this.and.bind(this);
    this.transform = this.transform.bind(this);
    this.brand = this.brand.bind(this);
    this.default = this.default.bind(this);
    this.catch = this.catch.bind(this);
    this.describe = this.describe.bind(this);
    this.pipe = this.pipe.bind(this);
    this.readonly = this.readonly.bind(this);
    this.isNullable = this.isNullable.bind(this);
    this.isOptional = this.isOptional.bind(this);
    this["~standard"] = {
      version: 1,
      vendor: "zod",
      validate: /* @__PURE__ */ __name((data) => this["~validate"](data), "validate")
    };
  }
  optional() {
    return ZodOptional.create(this, this._def);
  }
  nullable() {
    return ZodNullable.create(this, this._def);
  }
  nullish() {
    return this.nullable().optional();
  }
  array() {
    return ZodArray.create(this);
  }
  promise() {
    return ZodPromise.create(this, this._def);
  }
  or(option) {
    return ZodUnion.create([this, option], this._def);
  }
  and(incoming) {
    return ZodIntersection.create(this, incoming, this._def);
  }
  transform(transform) {
    return new ZodEffects({
      ...processCreateParams(this._def),
      schema: this,
      typeName: ZodFirstPartyTypeKind.ZodEffects,
      effect: { type: "transform", transform }
    });
  }
  default(def) {
    const defaultValueFunc = typeof def === "function" ? def : () => def;
    return new ZodDefault({
      ...processCreateParams(this._def),
      innerType: this,
      defaultValue: defaultValueFunc,
      typeName: ZodFirstPartyTypeKind.ZodDefault
    });
  }
  brand() {
    return new ZodBranded({
      typeName: ZodFirstPartyTypeKind.ZodBranded,
      type: this,
      ...processCreateParams(this._def)
    });
  }
  catch(def) {
    const catchValueFunc = typeof def === "function" ? def : () => def;
    return new ZodCatch({
      ...processCreateParams(this._def),
      innerType: this,
      catchValue: catchValueFunc,
      typeName: ZodFirstPartyTypeKind.ZodCatch
    });
  }
  describe(description) {
    const This = this.constructor;
    return new This({
      ...this._def,
      description
    });
  }
  pipe(target) {
    return ZodPipeline.create(this, target);
  }
  readonly() {
    return ZodReadonly.create(this);
  }
  isOptional() {
    return this.safeParse(void 0).success;
  }
  isNullable() {
    return this.safeParse(null).success;
  }
};
var cuidRegex = /^c[^\s-]{8,}$/i;
var cuid2Regex = /^[0-9a-z]+$/;
var ulidRegex = /^[0-9A-HJKMNP-TV-Z]{26}$/i;
var uuidRegex = /^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/i;
var nanoidRegex = /^[a-z0-9_-]{21}$/i;
var jwtRegex = /^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$/;
var durationRegex = /^[-+]?P(?!$)(?:(?:[-+]?\d+Y)|(?:[-+]?\d+[.,]\d+Y$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:(?:[-+]?\d+W)|(?:[-+]?\d+[.,]\d+W$))?(?:(?:[-+]?\d+D)|(?:[-+]?\d+[.,]\d+D$))?(?:T(?=[\d+-])(?:(?:[-+]?\d+H)|(?:[-+]?\d+[.,]\d+H$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:[-+]?\d+(?:[.,]\d+)?S)?)??$/;
var emailRegex = /^(?!\.)(?!.*\.\.)([A-Z0-9_'+\-\.]*)[A-Z0-9_+-]@([A-Z0-9][A-Z0-9\-]*\.)+[A-Z]{2,}$/i;
var _emojiRegex = `^(\\p{Extended_Pictographic}|\\p{Emoji_Component})+$`;
var emojiRegex;
var ipv4Regex = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
var ipv4CidrRegex = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/(3[0-2]|[12]?[0-9])$/;
var ipv6Regex = /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$/;
var ipv6CidrRegex = /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
var base64Regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
var base64urlRegex = /^([0-9a-zA-Z-_]{4})*(([0-9a-zA-Z-_]{2}(==)?)|([0-9a-zA-Z-_]{3}(=)?))?$/;
var dateRegexSource = `((\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-((0[13578]|1[02])-(0[1-9]|[12]\\d|3[01])|(0[469]|11)-(0[1-9]|[12]\\d|30)|(02)-(0[1-9]|1\\d|2[0-8])))`;
var dateRegex = new RegExp(`^${dateRegexSource}$`);
function timeRegexSource(args) {
  let regex = `([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d`;
  if (args.precision) {
    regex = `${regex}\\.\\d{${args.precision}}`;
  } else if (args.precision == null) {
    regex = `${regex}(\\.\\d+)?`;
  }
  return regex;
}
__name(timeRegexSource, "timeRegexSource");
function timeRegex(args) {
  return new RegExp(`^${timeRegexSource(args)}$`);
}
__name(timeRegex, "timeRegex");
function datetimeRegex(args) {
  let regex = `${dateRegexSource}T${timeRegexSource(args)}`;
  const opts = [];
  opts.push(args.local ? `Z?` : `Z`);
  if (args.offset)
    opts.push(`([+-]\\d{2}:?\\d{2})`);
  regex = `${regex}(${opts.join("|")})`;
  return new RegExp(`^${regex}$`);
}
__name(datetimeRegex, "datetimeRegex");
function isValidIP(ip, version2) {
  if ((version2 === "v4" || !version2) && ipv4Regex.test(ip)) {
    return true;
  }
  if ((version2 === "v6" || !version2) && ipv6Regex.test(ip)) {
    return true;
  }
  return false;
}
__name(isValidIP, "isValidIP");
function isValidJWT(jwt, alg) {
  if (!jwtRegex.test(jwt))
    return false;
  try {
    const [header] = jwt.split(".");
    const base64 = header.replace(/-/g, "+").replace(/_/g, "/").padEnd(header.length + (4 - header.length % 4) % 4, "=");
    const decoded = JSON.parse(atob(base64));
    if (typeof decoded !== "object" || decoded === null)
      return false;
    if (!decoded.typ || !decoded.alg)
      return false;
    if (alg && decoded.alg !== alg)
      return false;
    return true;
  } catch (_a) {
    return false;
  }
}
__name(isValidJWT, "isValidJWT");
function isValidCidr(ip, version2) {
  if ((version2 === "v4" || !version2) && ipv4CidrRegex.test(ip)) {
    return true;
  }
  if ((version2 === "v6" || !version2) && ipv6CidrRegex.test(ip)) {
    return true;
  }
  return false;
}
__name(isValidCidr, "isValidCidr");
var ZodString = class _ZodString extends ZodType {
  static {
    __name(this, "ZodString");
  }
  _parse(input) {
    if (this._def.coerce) {
      input.data = String(input.data);
    }
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.string) {
      const ctx2 = this._getOrReturnCtx(input);
      addIssueToContext(ctx2, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.string,
        received: ctx2.parsedType
      });
      return INVALID;
    }
    const status = new ParseStatus();
    let ctx = void 0;
    for (const check of this._def.checks) {
      if (check.kind === "min") {
        if (input.data.length < check.value) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_small,
            minimum: check.value,
            type: "string",
            inclusive: true,
            exact: false,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "max") {
        if (input.data.length > check.value) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_big,
            maximum: check.value,
            type: "string",
            inclusive: true,
            exact: false,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "length") {
        const tooBig = input.data.length > check.value;
        const tooSmall = input.data.length < check.value;
        if (tooBig || tooSmall) {
          ctx = this._getOrReturnCtx(input, ctx);
          if (tooBig) {
            addIssueToContext(ctx, {
              code: ZodIssueCode.too_big,
              maximum: check.value,
              type: "string",
              inclusive: true,
              exact: true,
              message: check.message
            });
          } else if (tooSmall) {
            addIssueToContext(ctx, {
              code: ZodIssueCode.too_small,
              minimum: check.value,
              type: "string",
              inclusive: true,
              exact: true,
              message: check.message
            });
          }
          status.dirty();
        }
      } else if (check.kind === "email") {
        if (!emailRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "email",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "emoji") {
        if (!emojiRegex) {
          emojiRegex = new RegExp(_emojiRegex, "u");
        }
        if (!emojiRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "emoji",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "uuid") {
        if (!uuidRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "uuid",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "nanoid") {
        if (!nanoidRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "nanoid",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "cuid") {
        if (!cuidRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "cuid",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "cuid2") {
        if (!cuid2Regex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "cuid2",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "ulid") {
        if (!ulidRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "ulid",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "url") {
        try {
          new URL(input.data);
        } catch (_a) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "url",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "regex") {
        check.regex.lastIndex = 0;
        const testResult = check.regex.test(input.data);
        if (!testResult) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "regex",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "trim") {
        input.data = input.data.trim();
      } else if (check.kind === "includes") {
        if (!input.data.includes(check.value, check.position)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: { includes: check.value, position: check.position },
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "toLowerCase") {
        input.data = input.data.toLowerCase();
      } else if (check.kind === "toUpperCase") {
        input.data = input.data.toUpperCase();
      } else if (check.kind === "startsWith") {
        if (!input.data.startsWith(check.value)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: { startsWith: check.value },
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "endsWith") {
        if (!input.data.endsWith(check.value)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: { endsWith: check.value },
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "datetime") {
        const regex = datetimeRegex(check);
        if (!regex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: "datetime",
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "date") {
        const regex = dateRegex;
        if (!regex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: "date",
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "time") {
        const regex = timeRegex(check);
        if (!regex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_string,
            validation: "time",
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "duration") {
        if (!durationRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "duration",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "ip") {
        if (!isValidIP(input.data, check.version)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "ip",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "jwt") {
        if (!isValidJWT(input.data, check.alg)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "jwt",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "cidr") {
        if (!isValidCidr(input.data, check.version)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "cidr",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "base64") {
        if (!base64Regex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "base64",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "base64url") {
        if (!base64urlRegex.test(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            validation: "base64url",
            code: ZodIssueCode.invalid_string,
            message: check.message
          });
          status.dirty();
        }
      } else {
        util.assertNever(check);
      }
    }
    return { status: status.value, value: input.data };
  }
  _regex(regex, validation, message) {
    return this.refinement((data) => regex.test(data), {
      validation,
      code: ZodIssueCode.invalid_string,
      ...errorUtil.errToObj(message)
    });
  }
  _addCheck(check) {
    return new _ZodString({
      ...this._def,
      checks: [...this._def.checks, check]
    });
  }
  email(message) {
    return this._addCheck({ kind: "email", ...errorUtil.errToObj(message) });
  }
  url(message) {
    return this._addCheck({ kind: "url", ...errorUtil.errToObj(message) });
  }
  emoji(message) {
    return this._addCheck({ kind: "emoji", ...errorUtil.errToObj(message) });
  }
  uuid(message) {
    return this._addCheck({ kind: "uuid", ...errorUtil.errToObj(message) });
  }
  nanoid(message) {
    return this._addCheck({ kind: "nanoid", ...errorUtil.errToObj(message) });
  }
  cuid(message) {
    return this._addCheck({ kind: "cuid", ...errorUtil.errToObj(message) });
  }
  cuid2(message) {
    return this._addCheck({ kind: "cuid2", ...errorUtil.errToObj(message) });
  }
  ulid(message) {
    return this._addCheck({ kind: "ulid", ...errorUtil.errToObj(message) });
  }
  base64(message) {
    return this._addCheck({ kind: "base64", ...errorUtil.errToObj(message) });
  }
  base64url(message) {
    return this._addCheck({
      kind: "base64url",
      ...errorUtil.errToObj(message)
    });
  }
  jwt(options) {
    return this._addCheck({ kind: "jwt", ...errorUtil.errToObj(options) });
  }
  ip(options) {
    return this._addCheck({ kind: "ip", ...errorUtil.errToObj(options) });
  }
  cidr(options) {
    return this._addCheck({ kind: "cidr", ...errorUtil.errToObj(options) });
  }
  datetime(options) {
    var _a, _b;
    if (typeof options === "string") {
      return this._addCheck({
        kind: "datetime",
        precision: null,
        offset: false,
        local: false,
        message: options
      });
    }
    return this._addCheck({
      kind: "datetime",
      precision: typeof (options === null || options === void 0 ? void 0 : options.precision) === "undefined" ? null : options === null || options === void 0 ? void 0 : options.precision,
      offset: (_a = options === null || options === void 0 ? void 0 : options.offset) !== null && _a !== void 0 ? _a : false,
      local: (_b = options === null || options === void 0 ? void 0 : options.local) !== null && _b !== void 0 ? _b : false,
      ...errorUtil.errToObj(options === null || options === void 0 ? void 0 : options.message)
    });
  }
  date(message) {
    return this._addCheck({ kind: "date", message });
  }
  time(options) {
    if (typeof options === "string") {
      return this._addCheck({
        kind: "time",
        precision: null,
        message: options
      });
    }
    return this._addCheck({
      kind: "time",
      precision: typeof (options === null || options === void 0 ? void 0 : options.precision) === "undefined" ? null : options === null || options === void 0 ? void 0 : options.precision,
      ...errorUtil.errToObj(options === null || options === void 0 ? void 0 : options.message)
    });
  }
  duration(message) {
    return this._addCheck({ kind: "duration", ...errorUtil.errToObj(message) });
  }
  regex(regex, message) {
    return this._addCheck({
      kind: "regex",
      regex,
      ...errorUtil.errToObj(message)
    });
  }
  includes(value, options) {
    return this._addCheck({
      kind: "includes",
      value,
      position: options === null || options === void 0 ? void 0 : options.position,
      ...errorUtil.errToObj(options === null || options === void 0 ? void 0 : options.message)
    });
  }
  startsWith(value, message) {
    return this._addCheck({
      kind: "startsWith",
      value,
      ...errorUtil.errToObj(message)
    });
  }
  endsWith(value, message) {
    return this._addCheck({
      kind: "endsWith",
      value,
      ...errorUtil.errToObj(message)
    });
  }
  min(minLength, message) {
    return this._addCheck({
      kind: "min",
      value: minLength,
      ...errorUtil.errToObj(message)
    });
  }
  max(maxLength, message) {
    return this._addCheck({
      kind: "max",
      value: maxLength,
      ...errorUtil.errToObj(message)
    });
  }
  length(len, message) {
    return this._addCheck({
      kind: "length",
      value: len,
      ...errorUtil.errToObj(message)
    });
  }
  /**
   * Equivalent to `.min(1)`
   */
  nonempty(message) {
    return this.min(1, errorUtil.errToObj(message));
  }
  trim() {
    return new _ZodString({
      ...this._def,
      checks: [...this._def.checks, { kind: "trim" }]
    });
  }
  toLowerCase() {
    return new _ZodString({
      ...this._def,
      checks: [...this._def.checks, { kind: "toLowerCase" }]
    });
  }
  toUpperCase() {
    return new _ZodString({
      ...this._def,
      checks: [...this._def.checks, { kind: "toUpperCase" }]
    });
  }
  get isDatetime() {
    return !!this._def.checks.find((ch) => ch.kind === "datetime");
  }
  get isDate() {
    return !!this._def.checks.find((ch) => ch.kind === "date");
  }
  get isTime() {
    return !!this._def.checks.find((ch) => ch.kind === "time");
  }
  get isDuration() {
    return !!this._def.checks.find((ch) => ch.kind === "duration");
  }
  get isEmail() {
    return !!this._def.checks.find((ch) => ch.kind === "email");
  }
  get isURL() {
    return !!this._def.checks.find((ch) => ch.kind === "url");
  }
  get isEmoji() {
    return !!this._def.checks.find((ch) => ch.kind === "emoji");
  }
  get isUUID() {
    return !!this._def.checks.find((ch) => ch.kind === "uuid");
  }
  get isNANOID() {
    return !!this._def.checks.find((ch) => ch.kind === "nanoid");
  }
  get isCUID() {
    return !!this._def.checks.find((ch) => ch.kind === "cuid");
  }
  get isCUID2() {
    return !!this._def.checks.find((ch) => ch.kind === "cuid2");
  }
  get isULID() {
    return !!this._def.checks.find((ch) => ch.kind === "ulid");
  }
  get isIP() {
    return !!this._def.checks.find((ch) => ch.kind === "ip");
  }
  get isCIDR() {
    return !!this._def.checks.find((ch) => ch.kind === "cidr");
  }
  get isBase64() {
    return !!this._def.checks.find((ch) => ch.kind === "base64");
  }
  get isBase64url() {
    return !!this._def.checks.find((ch) => ch.kind === "base64url");
  }
  get minLength() {
    let min = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "min") {
        if (min === null || ch.value > min)
          min = ch.value;
      }
    }
    return min;
  }
  get maxLength() {
    let max = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "max") {
        if (max === null || ch.value < max)
          max = ch.value;
      }
    }
    return max;
  }
};
ZodString.create = (params) => {
  var _a;
  return new ZodString({
    checks: [],
    typeName: ZodFirstPartyTypeKind.ZodString,
    coerce: (_a = params === null || params === void 0 ? void 0 : params.coerce) !== null && _a !== void 0 ? _a : false,
    ...processCreateParams(params)
  });
};
function floatSafeRemainder(val, step) {
  const valDecCount = (val.toString().split(".")[1] || "").length;
  const stepDecCount = (step.toString().split(".")[1] || "").length;
  const decCount = valDecCount > stepDecCount ? valDecCount : stepDecCount;
  const valInt = parseInt(val.toFixed(decCount).replace(".", ""));
  const stepInt = parseInt(step.toFixed(decCount).replace(".", ""));
  return valInt % stepInt / Math.pow(10, decCount);
}
__name(floatSafeRemainder, "floatSafeRemainder");
var ZodNumber = class _ZodNumber extends ZodType {
  static {
    __name(this, "ZodNumber");
  }
  constructor() {
    super(...arguments);
    this.min = this.gte;
    this.max = this.lte;
    this.step = this.multipleOf;
  }
  _parse(input) {
    if (this._def.coerce) {
      input.data = Number(input.data);
    }
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.number) {
      const ctx2 = this._getOrReturnCtx(input);
      addIssueToContext(ctx2, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.number,
        received: ctx2.parsedType
      });
      return INVALID;
    }
    let ctx = void 0;
    const status = new ParseStatus();
    for (const check of this._def.checks) {
      if (check.kind === "int") {
        if (!util.isInteger(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.invalid_type,
            expected: "integer",
            received: "float",
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "min") {
        const tooSmall = check.inclusive ? input.data < check.value : input.data <= check.value;
        if (tooSmall) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_small,
            minimum: check.value,
            type: "number",
            inclusive: check.inclusive,
            exact: false,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "max") {
        const tooBig = check.inclusive ? input.data > check.value : input.data >= check.value;
        if (tooBig) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_big,
            maximum: check.value,
            type: "number",
            inclusive: check.inclusive,
            exact: false,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "multipleOf") {
        if (floatSafeRemainder(input.data, check.value) !== 0) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.not_multiple_of,
            multipleOf: check.value,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "finite") {
        if (!Number.isFinite(input.data)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.not_finite,
            message: check.message
          });
          status.dirty();
        }
      } else {
        util.assertNever(check);
      }
    }
    return { status: status.value, value: input.data };
  }
  gte(value, message) {
    return this.setLimit("min", value, true, errorUtil.toString(message));
  }
  gt(value, message) {
    return this.setLimit("min", value, false, errorUtil.toString(message));
  }
  lte(value, message) {
    return this.setLimit("max", value, true, errorUtil.toString(message));
  }
  lt(value, message) {
    return this.setLimit("max", value, false, errorUtil.toString(message));
  }
  setLimit(kind, value, inclusive, message) {
    return new _ZodNumber({
      ...this._def,
      checks: [
        ...this._def.checks,
        {
          kind,
          value,
          inclusive,
          message: errorUtil.toString(message)
        }
      ]
    });
  }
  _addCheck(check) {
    return new _ZodNumber({
      ...this._def,
      checks: [...this._def.checks, check]
    });
  }
  int(message) {
    return this._addCheck({
      kind: "int",
      message: errorUtil.toString(message)
    });
  }
  positive(message) {
    return this._addCheck({
      kind: "min",
      value: 0,
      inclusive: false,
      message: errorUtil.toString(message)
    });
  }
  negative(message) {
    return this._addCheck({
      kind: "max",
      value: 0,
      inclusive: false,
      message: errorUtil.toString(message)
    });
  }
  nonpositive(message) {
    return this._addCheck({
      kind: "max",
      value: 0,
      inclusive: true,
      message: errorUtil.toString(message)
    });
  }
  nonnegative(message) {
    return this._addCheck({
      kind: "min",
      value: 0,
      inclusive: true,
      message: errorUtil.toString(message)
    });
  }
  multipleOf(value, message) {
    return this._addCheck({
      kind: "multipleOf",
      value,
      message: errorUtil.toString(message)
    });
  }
  finite(message) {
    return this._addCheck({
      kind: "finite",
      message: errorUtil.toString(message)
    });
  }
  safe(message) {
    return this._addCheck({
      kind: "min",
      inclusive: true,
      value: Number.MIN_SAFE_INTEGER,
      message: errorUtil.toString(message)
    })._addCheck({
      kind: "max",
      inclusive: true,
      value: Number.MAX_SAFE_INTEGER,
      message: errorUtil.toString(message)
    });
  }
  get minValue() {
    let min = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "min") {
        if (min === null || ch.value > min)
          min = ch.value;
      }
    }
    return min;
  }
  get maxValue() {
    let max = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "max") {
        if (max === null || ch.value < max)
          max = ch.value;
      }
    }
    return max;
  }
  get isInt() {
    return !!this._def.checks.find((ch) => ch.kind === "int" || ch.kind === "multipleOf" && util.isInteger(ch.value));
  }
  get isFinite() {
    let max = null, min = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "finite" || ch.kind === "int" || ch.kind === "multipleOf") {
        return true;
      } else if (ch.kind === "min") {
        if (min === null || ch.value > min)
          min = ch.value;
      } else if (ch.kind === "max") {
        if (max === null || ch.value < max)
          max = ch.value;
      }
    }
    return Number.isFinite(min) && Number.isFinite(max);
  }
};
ZodNumber.create = (params) => {
  return new ZodNumber({
    checks: [],
    typeName: ZodFirstPartyTypeKind.ZodNumber,
    coerce: (params === null || params === void 0 ? void 0 : params.coerce) || false,
    ...processCreateParams(params)
  });
};
var ZodBigInt = class _ZodBigInt extends ZodType {
  static {
    __name(this, "ZodBigInt");
  }
  constructor() {
    super(...arguments);
    this.min = this.gte;
    this.max = this.lte;
  }
  _parse(input) {
    if (this._def.coerce) {
      try {
        input.data = BigInt(input.data);
      } catch (_a) {
        return this._getInvalidInput(input);
      }
    }
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.bigint) {
      return this._getInvalidInput(input);
    }
    let ctx = void 0;
    const status = new ParseStatus();
    for (const check of this._def.checks) {
      if (check.kind === "min") {
        const tooSmall = check.inclusive ? input.data < check.value : input.data <= check.value;
        if (tooSmall) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_small,
            type: "bigint",
            minimum: check.value,
            inclusive: check.inclusive,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "max") {
        const tooBig = check.inclusive ? input.data > check.value : input.data >= check.value;
        if (tooBig) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_big,
            type: "bigint",
            maximum: check.value,
            inclusive: check.inclusive,
            message: check.message
          });
          status.dirty();
        }
      } else if (check.kind === "multipleOf") {
        if (input.data % check.value !== BigInt(0)) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.not_multiple_of,
            multipleOf: check.value,
            message: check.message
          });
          status.dirty();
        }
      } else {
        util.assertNever(check);
      }
    }
    return { status: status.value, value: input.data };
  }
  _getInvalidInput(input) {
    const ctx = this._getOrReturnCtx(input);
    addIssueToContext(ctx, {
      code: ZodIssueCode.invalid_type,
      expected: ZodParsedType.bigint,
      received: ctx.parsedType
    });
    return INVALID;
  }
  gte(value, message) {
    return this.setLimit("min", value, true, errorUtil.toString(message));
  }
  gt(value, message) {
    return this.setLimit("min", value, false, errorUtil.toString(message));
  }
  lte(value, message) {
    return this.setLimit("max", value, true, errorUtil.toString(message));
  }
  lt(value, message) {
    return this.setLimit("max", value, false, errorUtil.toString(message));
  }
  setLimit(kind, value, inclusive, message) {
    return new _ZodBigInt({
      ...this._def,
      checks: [
        ...this._def.checks,
        {
          kind,
          value,
          inclusive,
          message: errorUtil.toString(message)
        }
      ]
    });
  }
  _addCheck(check) {
    return new _ZodBigInt({
      ...this._def,
      checks: [...this._def.checks, check]
    });
  }
  positive(message) {
    return this._addCheck({
      kind: "min",
      value: BigInt(0),
      inclusive: false,
      message: errorUtil.toString(message)
    });
  }
  negative(message) {
    return this._addCheck({
      kind: "max",
      value: BigInt(0),
      inclusive: false,
      message: errorUtil.toString(message)
    });
  }
  nonpositive(message) {
    return this._addCheck({
      kind: "max",
      value: BigInt(0),
      inclusive: true,
      message: errorUtil.toString(message)
    });
  }
  nonnegative(message) {
    return this._addCheck({
      kind: "min",
      value: BigInt(0),
      inclusive: true,
      message: errorUtil.toString(message)
    });
  }
  multipleOf(value, message) {
    return this._addCheck({
      kind: "multipleOf",
      value,
      message: errorUtil.toString(message)
    });
  }
  get minValue() {
    let min = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "min") {
        if (min === null || ch.value > min)
          min = ch.value;
      }
    }
    return min;
  }
  get maxValue() {
    let max = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "max") {
        if (max === null || ch.value < max)
          max = ch.value;
      }
    }
    return max;
  }
};
ZodBigInt.create = (params) => {
  var _a;
  return new ZodBigInt({
    checks: [],
    typeName: ZodFirstPartyTypeKind.ZodBigInt,
    coerce: (_a = params === null || params === void 0 ? void 0 : params.coerce) !== null && _a !== void 0 ? _a : false,
    ...processCreateParams(params)
  });
};
var ZodBoolean = class extends ZodType {
  static {
    __name(this, "ZodBoolean");
  }
  _parse(input) {
    if (this._def.coerce) {
      input.data = Boolean(input.data);
    }
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.boolean) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.boolean,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return OK(input.data);
  }
};
ZodBoolean.create = (params) => {
  return new ZodBoolean({
    typeName: ZodFirstPartyTypeKind.ZodBoolean,
    coerce: (params === null || params === void 0 ? void 0 : params.coerce) || false,
    ...processCreateParams(params)
  });
};
var ZodDate = class _ZodDate extends ZodType {
  static {
    __name(this, "ZodDate");
  }
  _parse(input) {
    if (this._def.coerce) {
      input.data = new Date(input.data);
    }
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.date) {
      const ctx2 = this._getOrReturnCtx(input);
      addIssueToContext(ctx2, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.date,
        received: ctx2.parsedType
      });
      return INVALID;
    }
    if (isNaN(input.data.getTime())) {
      const ctx2 = this._getOrReturnCtx(input);
      addIssueToContext(ctx2, {
        code: ZodIssueCode.invalid_date
      });
      return INVALID;
    }
    const status = new ParseStatus();
    let ctx = void 0;
    for (const check of this._def.checks) {
      if (check.kind === "min") {
        if (input.data.getTime() < check.value) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_small,
            message: check.message,
            inclusive: true,
            exact: false,
            minimum: check.value,
            type: "date"
          });
          status.dirty();
        }
      } else if (check.kind === "max") {
        if (input.data.getTime() > check.value) {
          ctx = this._getOrReturnCtx(input, ctx);
          addIssueToContext(ctx, {
            code: ZodIssueCode.too_big,
            message: check.message,
            inclusive: true,
            exact: false,
            maximum: check.value,
            type: "date"
          });
          status.dirty();
        }
      } else {
        util.assertNever(check);
      }
    }
    return {
      status: status.value,
      value: new Date(input.data.getTime())
    };
  }
  _addCheck(check) {
    return new _ZodDate({
      ...this._def,
      checks: [...this._def.checks, check]
    });
  }
  min(minDate, message) {
    return this._addCheck({
      kind: "min",
      value: minDate.getTime(),
      message: errorUtil.toString(message)
    });
  }
  max(maxDate, message) {
    return this._addCheck({
      kind: "max",
      value: maxDate.getTime(),
      message: errorUtil.toString(message)
    });
  }
  get minDate() {
    let min = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "min") {
        if (min === null || ch.value > min)
          min = ch.value;
      }
    }
    return min != null ? new Date(min) : null;
  }
  get maxDate() {
    let max = null;
    for (const ch of this._def.checks) {
      if (ch.kind === "max") {
        if (max === null || ch.value < max)
          max = ch.value;
      }
    }
    return max != null ? new Date(max) : null;
  }
};
ZodDate.create = (params) => {
  return new ZodDate({
    checks: [],
    coerce: (params === null || params === void 0 ? void 0 : params.coerce) || false,
    typeName: ZodFirstPartyTypeKind.ZodDate,
    ...processCreateParams(params)
  });
};
var ZodSymbol = class extends ZodType {
  static {
    __name(this, "ZodSymbol");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.symbol) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.symbol,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return OK(input.data);
  }
};
ZodSymbol.create = (params) => {
  return new ZodSymbol({
    typeName: ZodFirstPartyTypeKind.ZodSymbol,
    ...processCreateParams(params)
  });
};
var ZodUndefined = class extends ZodType {
  static {
    __name(this, "ZodUndefined");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.undefined) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.undefined,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return OK(input.data);
  }
};
ZodUndefined.create = (params) => {
  return new ZodUndefined({
    typeName: ZodFirstPartyTypeKind.ZodUndefined,
    ...processCreateParams(params)
  });
};
var ZodNull = class extends ZodType {
  static {
    __name(this, "ZodNull");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.null) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.null,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return OK(input.data);
  }
};
ZodNull.create = (params) => {
  return new ZodNull({
    typeName: ZodFirstPartyTypeKind.ZodNull,
    ...processCreateParams(params)
  });
};
var ZodAny = class extends ZodType {
  static {
    __name(this, "ZodAny");
  }
  constructor() {
    super(...arguments);
    this._any = true;
  }
  _parse(input) {
    return OK(input.data);
  }
};
ZodAny.create = (params) => {
  return new ZodAny({
    typeName: ZodFirstPartyTypeKind.ZodAny,
    ...processCreateParams(params)
  });
};
var ZodUnknown = class extends ZodType {
  static {
    __name(this, "ZodUnknown");
  }
  constructor() {
    super(...arguments);
    this._unknown = true;
  }
  _parse(input) {
    return OK(input.data);
  }
};
ZodUnknown.create = (params) => {
  return new ZodUnknown({
    typeName: ZodFirstPartyTypeKind.ZodUnknown,
    ...processCreateParams(params)
  });
};
var ZodNever = class extends ZodType {
  static {
    __name(this, "ZodNever");
  }
  _parse(input) {
    const ctx = this._getOrReturnCtx(input);
    addIssueToContext(ctx, {
      code: ZodIssueCode.invalid_type,
      expected: ZodParsedType.never,
      received: ctx.parsedType
    });
    return INVALID;
  }
};
ZodNever.create = (params) => {
  return new ZodNever({
    typeName: ZodFirstPartyTypeKind.ZodNever,
    ...processCreateParams(params)
  });
};
var ZodVoid = class extends ZodType {
  static {
    __name(this, "ZodVoid");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.undefined) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.void,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return OK(input.data);
  }
};
ZodVoid.create = (params) => {
  return new ZodVoid({
    typeName: ZodFirstPartyTypeKind.ZodVoid,
    ...processCreateParams(params)
  });
};
var ZodArray = class _ZodArray extends ZodType {
  static {
    __name(this, "ZodArray");
  }
  _parse(input) {
    const { ctx, status } = this._processInputParams(input);
    const def = this._def;
    if (ctx.parsedType !== ZodParsedType.array) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.array,
        received: ctx.parsedType
      });
      return INVALID;
    }
    if (def.exactLength !== null) {
      const tooBig = ctx.data.length > def.exactLength.value;
      const tooSmall = ctx.data.length < def.exactLength.value;
      if (tooBig || tooSmall) {
        addIssueToContext(ctx, {
          code: tooBig ? ZodIssueCode.too_big : ZodIssueCode.too_small,
          minimum: tooSmall ? def.exactLength.value : void 0,
          maximum: tooBig ? def.exactLength.value : void 0,
          type: "array",
          inclusive: true,
          exact: true,
          message: def.exactLength.message
        });
        status.dirty();
      }
    }
    if (def.minLength !== null) {
      if (ctx.data.length < def.minLength.value) {
        addIssueToContext(ctx, {
          code: ZodIssueCode.too_small,
          minimum: def.minLength.value,
          type: "array",
          inclusive: true,
          exact: false,
          message: def.minLength.message
        });
        status.dirty();
      }
    }
    if (def.maxLength !== null) {
      if (ctx.data.length > def.maxLength.value) {
        addIssueToContext(ctx, {
          code: ZodIssueCode.too_big,
          maximum: def.maxLength.value,
          type: "array",
          inclusive: true,
          exact: false,
          message: def.maxLength.message
        });
        status.dirty();
      }
    }
    if (ctx.common.async) {
      return Promise.all([...ctx.data].map((item, i) => {
        return def.type._parseAsync(new ParseInputLazyPath(ctx, item, ctx.path, i));
      })).then((result2) => {
        return ParseStatus.mergeArray(status, result2);
      });
    }
    const result = [...ctx.data].map((item, i) => {
      return def.type._parseSync(new ParseInputLazyPath(ctx, item, ctx.path, i));
    });
    return ParseStatus.mergeArray(status, result);
  }
  get element() {
    return this._def.type;
  }
  min(minLength, message) {
    return new _ZodArray({
      ...this._def,
      minLength: { value: minLength, message: errorUtil.toString(message) }
    });
  }
  max(maxLength, message) {
    return new _ZodArray({
      ...this._def,
      maxLength: { value: maxLength, message: errorUtil.toString(message) }
    });
  }
  length(len, message) {
    return new _ZodArray({
      ...this._def,
      exactLength: { value: len, message: errorUtil.toString(message) }
    });
  }
  nonempty(message) {
    return this.min(1, message);
  }
};
ZodArray.create = (schema, params) => {
  return new ZodArray({
    type: schema,
    minLength: null,
    maxLength: null,
    exactLength: null,
    typeName: ZodFirstPartyTypeKind.ZodArray,
    ...processCreateParams(params)
  });
};
function deepPartialify(schema) {
  if (schema instanceof ZodObject) {
    const newShape = {};
    for (const key in schema.shape) {
      const fieldSchema = schema.shape[key];
      newShape[key] = ZodOptional.create(deepPartialify(fieldSchema));
    }
    return new ZodObject({
      ...schema._def,
      shape: /* @__PURE__ */ __name(() => newShape, "shape")
    });
  } else if (schema instanceof ZodArray) {
    return new ZodArray({
      ...schema._def,
      type: deepPartialify(schema.element)
    });
  } else if (schema instanceof ZodOptional) {
    return ZodOptional.create(deepPartialify(schema.unwrap()));
  } else if (schema instanceof ZodNullable) {
    return ZodNullable.create(deepPartialify(schema.unwrap()));
  } else if (schema instanceof ZodTuple) {
    return ZodTuple.create(schema.items.map((item) => deepPartialify(item)));
  } else {
    return schema;
  }
}
__name(deepPartialify, "deepPartialify");
var ZodObject = class _ZodObject extends ZodType {
  static {
    __name(this, "ZodObject");
  }
  constructor() {
    super(...arguments);
    this._cached = null;
    this.nonstrict = this.passthrough;
    this.augment = this.extend;
  }
  _getCached() {
    if (this._cached !== null)
      return this._cached;
    const shape = this._def.shape();
    const keys = util.objectKeys(shape);
    return this._cached = { shape, keys };
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.object) {
      const ctx2 = this._getOrReturnCtx(input);
      addIssueToContext(ctx2, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.object,
        received: ctx2.parsedType
      });
      return INVALID;
    }
    const { status, ctx } = this._processInputParams(input);
    const { shape, keys: shapeKeys } = this._getCached();
    const extraKeys = [];
    if (!(this._def.catchall instanceof ZodNever && this._def.unknownKeys === "strip")) {
      for (const key in ctx.data) {
        if (!shapeKeys.includes(key)) {
          extraKeys.push(key);
        }
      }
    }
    const pairs = [];
    for (const key of shapeKeys) {
      const keyValidator = shape[key];
      const value = ctx.data[key];
      pairs.push({
        key: { status: "valid", value: key },
        value: keyValidator._parse(new ParseInputLazyPath(ctx, value, ctx.path, key)),
        alwaysSet: key in ctx.data
      });
    }
    if (this._def.catchall instanceof ZodNever) {
      const unknownKeys = this._def.unknownKeys;
      if (unknownKeys === "passthrough") {
        for (const key of extraKeys) {
          pairs.push({
            key: { status: "valid", value: key },
            value: { status: "valid", value: ctx.data[key] }
          });
        }
      } else if (unknownKeys === "strict") {
        if (extraKeys.length > 0) {
          addIssueToContext(ctx, {
            code: ZodIssueCode.unrecognized_keys,
            keys: extraKeys
          });
          status.dirty();
        }
      } else if (unknownKeys === "strip") ;
      else {
        throw new Error(`Internal ZodObject error: invalid unknownKeys value.`);
      }
    } else {
      const catchall = this._def.catchall;
      for (const key of extraKeys) {
        const value = ctx.data[key];
        pairs.push({
          key: { status: "valid", value: key },
          value: catchall._parse(
            new ParseInputLazyPath(ctx, value, ctx.path, key)
            //, ctx.child(key), value, getParsedType(value)
          ),
          alwaysSet: key in ctx.data
        });
      }
    }
    if (ctx.common.async) {
      return Promise.resolve().then(async () => {
        const syncPairs = [];
        for (const pair of pairs) {
          const key = await pair.key;
          const value = await pair.value;
          syncPairs.push({
            key,
            value,
            alwaysSet: pair.alwaysSet
          });
        }
        return syncPairs;
      }).then((syncPairs) => {
        return ParseStatus.mergeObjectSync(status, syncPairs);
      });
    } else {
      return ParseStatus.mergeObjectSync(status, pairs);
    }
  }
  get shape() {
    return this._def.shape();
  }
  strict(message) {
    errorUtil.errToObj;
    return new _ZodObject({
      ...this._def,
      unknownKeys: "strict",
      ...message !== void 0 ? {
        errorMap: /* @__PURE__ */ __name((issue, ctx) => {
          var _a, _b, _c, _d;
          const defaultError = (_c = (_b = (_a = this._def).errorMap) === null || _b === void 0 ? void 0 : _b.call(_a, issue, ctx).message) !== null && _c !== void 0 ? _c : ctx.defaultError;
          if (issue.code === "unrecognized_keys")
            return {
              message: (_d = errorUtil.errToObj(message).message) !== null && _d !== void 0 ? _d : defaultError
            };
          return {
            message: defaultError
          };
        }, "errorMap")
      } : {}
    });
  }
  strip() {
    return new _ZodObject({
      ...this._def,
      unknownKeys: "strip"
    });
  }
  passthrough() {
    return new _ZodObject({
      ...this._def,
      unknownKeys: "passthrough"
    });
  }
  // const AugmentFactory =
  //   <Def extends ZodObjectDef>(def: Def) =>
  //   <Augmentation extends ZodRawShape>(
  //     augmentation: Augmentation
  //   ): ZodObject<
  //     extendShape<ReturnType<Def["shape"]>, Augmentation>,
  //     Def["unknownKeys"],
  //     Def["catchall"]
  //   > => {
  //     return new ZodObject({
  //       ...def,
  //       shape: () => ({
  //         ...def.shape(),
  //         ...augmentation,
  //       }),
  //     }) as any;
  //   };
  extend(augmentation) {
    return new _ZodObject({
      ...this._def,
      shape: /* @__PURE__ */ __name(() => ({
        ...this._def.shape(),
        ...augmentation
      }), "shape")
    });
  }
  /**
   * Prior to zod@1.0.12 there was a bug in the
   * inferred type of merged objects. Please
   * upgrade if you are experiencing issues.
   */
  merge(merging) {
    const merged = new _ZodObject({
      unknownKeys: merging._def.unknownKeys,
      catchall: merging._def.catchall,
      shape: /* @__PURE__ */ __name(() => ({
        ...this._def.shape(),
        ...merging._def.shape()
      }), "shape"),
      typeName: ZodFirstPartyTypeKind.ZodObject
    });
    return merged;
  }
  // merge<
  //   Incoming extends AnyZodObject,
  //   Augmentation extends Incoming["shape"],
  //   NewOutput extends {
  //     [k in keyof Augmentation | keyof Output]: k extends keyof Augmentation
  //       ? Augmentation[k]["_output"]
  //       : k extends keyof Output
  //       ? Output[k]
  //       : never;
  //   },
  //   NewInput extends {
  //     [k in keyof Augmentation | keyof Input]: k extends keyof Augmentation
  //       ? Augmentation[k]["_input"]
  //       : k extends keyof Input
  //       ? Input[k]
  //       : never;
  //   }
  // >(
  //   merging: Incoming
  // ): ZodObject<
  //   extendShape<T, ReturnType<Incoming["_def"]["shape"]>>,
  //   Incoming["_def"]["unknownKeys"],
  //   Incoming["_def"]["catchall"],
  //   NewOutput,
  //   NewInput
  // > {
  //   const merged: any = new ZodObject({
  //     unknownKeys: merging._def.unknownKeys,
  //     catchall: merging._def.catchall,
  //     shape: () =>
  //       objectUtil.mergeShapes(this._def.shape(), merging._def.shape()),
  //     typeName: ZodFirstPartyTypeKind.ZodObject,
  //   }) as any;
  //   return merged;
  // }
  setKey(key, schema) {
    return this.augment({ [key]: schema });
  }
  // merge<Incoming extends AnyZodObject>(
  //   merging: Incoming
  // ): //ZodObject<T & Incoming["_shape"], UnknownKeys, Catchall> = (merging) => {
  // ZodObject<
  //   extendShape<T, ReturnType<Incoming["_def"]["shape"]>>,
  //   Incoming["_def"]["unknownKeys"],
  //   Incoming["_def"]["catchall"]
  // > {
  //   // const mergedShape = objectUtil.mergeShapes(
  //   //   this._def.shape(),
  //   //   merging._def.shape()
  //   // );
  //   const merged: any = new ZodObject({
  //     unknownKeys: merging._def.unknownKeys,
  //     catchall: merging._def.catchall,
  //     shape: () =>
  //       objectUtil.mergeShapes(this._def.shape(), merging._def.shape()),
  //     typeName: ZodFirstPartyTypeKind.ZodObject,
  //   }) as any;
  //   return merged;
  // }
  catchall(index) {
    return new _ZodObject({
      ...this._def,
      catchall: index
    });
  }
  pick(mask) {
    const shape = {};
    util.objectKeys(mask).forEach((key) => {
      if (mask[key] && this.shape[key]) {
        shape[key] = this.shape[key];
      }
    });
    return new _ZodObject({
      ...this._def,
      shape: /* @__PURE__ */ __name(() => shape, "shape")
    });
  }
  omit(mask) {
    const shape = {};
    util.objectKeys(this.shape).forEach((key) => {
      if (!mask[key]) {
        shape[key] = this.shape[key];
      }
    });
    return new _ZodObject({
      ...this._def,
      shape: /* @__PURE__ */ __name(() => shape, "shape")
    });
  }
  /**
   * @deprecated
   */
  deepPartial() {
    return deepPartialify(this);
  }
  partial(mask) {
    const newShape = {};
    util.objectKeys(this.shape).forEach((key) => {
      const fieldSchema = this.shape[key];
      if (mask && !mask[key]) {
        newShape[key] = fieldSchema;
      } else {
        newShape[key] = fieldSchema.optional();
      }
    });
    return new _ZodObject({
      ...this._def,
      shape: /* @__PURE__ */ __name(() => newShape, "shape")
    });
  }
  required(mask) {
    const newShape = {};
    util.objectKeys(this.shape).forEach((key) => {
      if (mask && !mask[key]) {
        newShape[key] = this.shape[key];
      } else {
        const fieldSchema = this.shape[key];
        let newField = fieldSchema;
        while (newField instanceof ZodOptional) {
          newField = newField._def.innerType;
        }
        newShape[key] = newField;
      }
    });
    return new _ZodObject({
      ...this._def,
      shape: /* @__PURE__ */ __name(() => newShape, "shape")
    });
  }
  keyof() {
    return createZodEnum(util.objectKeys(this.shape));
  }
};
ZodObject.create = (shape, params) => {
  return new ZodObject({
    shape: /* @__PURE__ */ __name(() => shape, "shape"),
    unknownKeys: "strip",
    catchall: ZodNever.create(),
    typeName: ZodFirstPartyTypeKind.ZodObject,
    ...processCreateParams(params)
  });
};
ZodObject.strictCreate = (shape, params) => {
  return new ZodObject({
    shape: /* @__PURE__ */ __name(() => shape, "shape"),
    unknownKeys: "strict",
    catchall: ZodNever.create(),
    typeName: ZodFirstPartyTypeKind.ZodObject,
    ...processCreateParams(params)
  });
};
ZodObject.lazycreate = (shape, params) => {
  return new ZodObject({
    shape,
    unknownKeys: "strip",
    catchall: ZodNever.create(),
    typeName: ZodFirstPartyTypeKind.ZodObject,
    ...processCreateParams(params)
  });
};
var ZodUnion = class extends ZodType {
  static {
    __name(this, "ZodUnion");
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    const options = this._def.options;
    function handleResults(results) {
      for (const result of results) {
        if (result.result.status === "valid") {
          return result.result;
        }
      }
      for (const result of results) {
        if (result.result.status === "dirty") {
          ctx.common.issues.push(...result.ctx.common.issues);
          return result.result;
        }
      }
      const unionErrors = results.map((result) => new ZodError(result.ctx.common.issues));
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_union,
        unionErrors
      });
      return INVALID;
    }
    __name(handleResults, "handleResults");
    if (ctx.common.async) {
      return Promise.all(options.map(async (option) => {
        const childCtx = {
          ...ctx,
          common: {
            ...ctx.common,
            issues: []
          },
          parent: null
        };
        return {
          result: await option._parseAsync({
            data: ctx.data,
            path: ctx.path,
            parent: childCtx
          }),
          ctx: childCtx
        };
      })).then(handleResults);
    } else {
      let dirty = void 0;
      const issues = [];
      for (const option of options) {
        const childCtx = {
          ...ctx,
          common: {
            ...ctx.common,
            issues: []
          },
          parent: null
        };
        const result = option._parseSync({
          data: ctx.data,
          path: ctx.path,
          parent: childCtx
        });
        if (result.status === "valid") {
          return result;
        } else if (result.status === "dirty" && !dirty) {
          dirty = { result, ctx: childCtx };
        }
        if (childCtx.common.issues.length) {
          issues.push(childCtx.common.issues);
        }
      }
      if (dirty) {
        ctx.common.issues.push(...dirty.ctx.common.issues);
        return dirty.result;
      }
      const unionErrors = issues.map((issues2) => new ZodError(issues2));
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_union,
        unionErrors
      });
      return INVALID;
    }
  }
  get options() {
    return this._def.options;
  }
};
ZodUnion.create = (types, params) => {
  return new ZodUnion({
    options: types,
    typeName: ZodFirstPartyTypeKind.ZodUnion,
    ...processCreateParams(params)
  });
};
var getDiscriminator = /* @__PURE__ */ __name((type) => {
  if (type instanceof ZodLazy) {
    return getDiscriminator(type.schema);
  } else if (type instanceof ZodEffects) {
    return getDiscriminator(type.innerType());
  } else if (type instanceof ZodLiteral) {
    return [type.value];
  } else if (type instanceof ZodEnum) {
    return type.options;
  } else if (type instanceof ZodNativeEnum) {
    return util.objectValues(type.enum);
  } else if (type instanceof ZodDefault) {
    return getDiscriminator(type._def.innerType);
  } else if (type instanceof ZodUndefined) {
    return [void 0];
  } else if (type instanceof ZodNull) {
    return [null];
  } else if (type instanceof ZodOptional) {
    return [void 0, ...getDiscriminator(type.unwrap())];
  } else if (type instanceof ZodNullable) {
    return [null, ...getDiscriminator(type.unwrap())];
  } else if (type instanceof ZodBranded) {
    return getDiscriminator(type.unwrap());
  } else if (type instanceof ZodReadonly) {
    return getDiscriminator(type.unwrap());
  } else if (type instanceof ZodCatch) {
    return getDiscriminator(type._def.innerType);
  } else {
    return [];
  }
}, "getDiscriminator");
var ZodDiscriminatedUnion = class _ZodDiscriminatedUnion extends ZodType {
  static {
    __name(this, "ZodDiscriminatedUnion");
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.object) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.object,
        received: ctx.parsedType
      });
      return INVALID;
    }
    const discriminator = this.discriminator;
    const discriminatorValue = ctx.data[discriminator];
    const option = this.optionsMap.get(discriminatorValue);
    if (!option) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_union_discriminator,
        options: Array.from(this.optionsMap.keys()),
        path: [discriminator]
      });
      return INVALID;
    }
    if (ctx.common.async) {
      return option._parseAsync({
        data: ctx.data,
        path: ctx.path,
        parent: ctx
      });
    } else {
      return option._parseSync({
        data: ctx.data,
        path: ctx.path,
        parent: ctx
      });
    }
  }
  get discriminator() {
    return this._def.discriminator;
  }
  get options() {
    return this._def.options;
  }
  get optionsMap() {
    return this._def.optionsMap;
  }
  /**
   * The constructor of the discriminated union schema. Its behaviour is very similar to that of the normal z.union() constructor.
   * However, it only allows a union of objects, all of which need to share a discriminator property. This property must
   * have a different value for each object in the union.
   * @param discriminator the name of the discriminator property
   * @param types an array of object schemas
   * @param params
   */
  static create(discriminator, options, params) {
    const optionsMap = /* @__PURE__ */ new Map();
    for (const type of options) {
      const discriminatorValues = getDiscriminator(type.shape[discriminator]);
      if (!discriminatorValues.length) {
        throw new Error(`A discriminator value for key \`${discriminator}\` could not be extracted from all schema options`);
      }
      for (const value of discriminatorValues) {
        if (optionsMap.has(value)) {
          throw new Error(`Discriminator property ${String(discriminator)} has duplicate value ${String(value)}`);
        }
        optionsMap.set(value, type);
      }
    }
    return new _ZodDiscriminatedUnion({
      typeName: ZodFirstPartyTypeKind.ZodDiscriminatedUnion,
      discriminator,
      options,
      optionsMap,
      ...processCreateParams(params)
    });
  }
};
function mergeValues(a, b) {
  const aType = getParsedType(a);
  const bType = getParsedType(b);
  if (a === b) {
    return { valid: true, data: a };
  } else if (aType === ZodParsedType.object && bType === ZodParsedType.object) {
    const bKeys = util.objectKeys(b);
    const sharedKeys = util.objectKeys(a).filter((key) => bKeys.indexOf(key) !== -1);
    const newObj = { ...a, ...b };
    for (const key of sharedKeys) {
      const sharedValue = mergeValues(a[key], b[key]);
      if (!sharedValue.valid) {
        return { valid: false };
      }
      newObj[key] = sharedValue.data;
    }
    return { valid: true, data: newObj };
  } else if (aType === ZodParsedType.array && bType === ZodParsedType.array) {
    if (a.length !== b.length) {
      return { valid: false };
    }
    const newArray = [];
    for (let index = 0; index < a.length; index++) {
      const itemA = a[index];
      const itemB = b[index];
      const sharedValue = mergeValues(itemA, itemB);
      if (!sharedValue.valid) {
        return { valid: false };
      }
      newArray.push(sharedValue.data);
    }
    return { valid: true, data: newArray };
  } else if (aType === ZodParsedType.date && bType === ZodParsedType.date && +a === +b) {
    return { valid: true, data: a };
  } else {
    return { valid: false };
  }
}
__name(mergeValues, "mergeValues");
var ZodIntersection = class extends ZodType {
  static {
    __name(this, "ZodIntersection");
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    const handleParsed = /* @__PURE__ */ __name((parsedLeft, parsedRight) => {
      if (isAborted(parsedLeft) || isAborted(parsedRight)) {
        return INVALID;
      }
      const merged = mergeValues(parsedLeft.value, parsedRight.value);
      if (!merged.valid) {
        addIssueToContext(ctx, {
          code: ZodIssueCode.invalid_intersection_types
        });
        return INVALID;
      }
      if (isDirty(parsedLeft) || isDirty(parsedRight)) {
        status.dirty();
      }
      return { status: status.value, value: merged.data };
    }, "handleParsed");
    if (ctx.common.async) {
      return Promise.all([
        this._def.left._parseAsync({
          data: ctx.data,
          path: ctx.path,
          parent: ctx
        }),
        this._def.right._parseAsync({
          data: ctx.data,
          path: ctx.path,
          parent: ctx
        })
      ]).then(([left, right]) => handleParsed(left, right));
    } else {
      return handleParsed(this._def.left._parseSync({
        data: ctx.data,
        path: ctx.path,
        parent: ctx
      }), this._def.right._parseSync({
        data: ctx.data,
        path: ctx.path,
        parent: ctx
      }));
    }
  }
};
ZodIntersection.create = (left, right, params) => {
  return new ZodIntersection({
    left,
    right,
    typeName: ZodFirstPartyTypeKind.ZodIntersection,
    ...processCreateParams(params)
  });
};
var ZodTuple = class _ZodTuple extends ZodType {
  static {
    __name(this, "ZodTuple");
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.array) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.array,
        received: ctx.parsedType
      });
      return INVALID;
    }
    if (ctx.data.length < this._def.items.length) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.too_small,
        minimum: this._def.items.length,
        inclusive: true,
        exact: false,
        type: "array"
      });
      return INVALID;
    }
    const rest = this._def.rest;
    if (!rest && ctx.data.length > this._def.items.length) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.too_big,
        maximum: this._def.items.length,
        inclusive: true,
        exact: false,
        type: "array"
      });
      status.dirty();
    }
    const items = [...ctx.data].map((item, itemIndex) => {
      const schema = this._def.items[itemIndex] || this._def.rest;
      if (!schema)
        return null;
      return schema._parse(new ParseInputLazyPath(ctx, item, ctx.path, itemIndex));
    }).filter((x) => !!x);
    if (ctx.common.async) {
      return Promise.all(items).then((results) => {
        return ParseStatus.mergeArray(status, results);
      });
    } else {
      return ParseStatus.mergeArray(status, items);
    }
  }
  get items() {
    return this._def.items;
  }
  rest(rest) {
    return new _ZodTuple({
      ...this._def,
      rest
    });
  }
};
ZodTuple.create = (schemas, params) => {
  if (!Array.isArray(schemas)) {
    throw new Error("You must pass an array of schemas to z.tuple([ ... ])");
  }
  return new ZodTuple({
    items: schemas,
    typeName: ZodFirstPartyTypeKind.ZodTuple,
    rest: null,
    ...processCreateParams(params)
  });
};
var ZodRecord = class _ZodRecord extends ZodType {
  static {
    __name(this, "ZodRecord");
  }
  get keySchema() {
    return this._def.keyType;
  }
  get valueSchema() {
    return this._def.valueType;
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.object) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.object,
        received: ctx.parsedType
      });
      return INVALID;
    }
    const pairs = [];
    const keyType = this._def.keyType;
    const valueType = this._def.valueType;
    for (const key in ctx.data) {
      pairs.push({
        key: keyType._parse(new ParseInputLazyPath(ctx, key, ctx.path, key)),
        value: valueType._parse(new ParseInputLazyPath(ctx, ctx.data[key], ctx.path, key)),
        alwaysSet: key in ctx.data
      });
    }
    if (ctx.common.async) {
      return ParseStatus.mergeObjectAsync(status, pairs);
    } else {
      return ParseStatus.mergeObjectSync(status, pairs);
    }
  }
  get element() {
    return this._def.valueType;
  }
  static create(first, second, third) {
    if (second instanceof ZodType) {
      return new _ZodRecord({
        keyType: first,
        valueType: second,
        typeName: ZodFirstPartyTypeKind.ZodRecord,
        ...processCreateParams(third)
      });
    }
    return new _ZodRecord({
      keyType: ZodString.create(),
      valueType: first,
      typeName: ZodFirstPartyTypeKind.ZodRecord,
      ...processCreateParams(second)
    });
  }
};
var ZodMap = class extends ZodType {
  static {
    __name(this, "ZodMap");
  }
  get keySchema() {
    return this._def.keyType;
  }
  get valueSchema() {
    return this._def.valueType;
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.map) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.map,
        received: ctx.parsedType
      });
      return INVALID;
    }
    const keyType = this._def.keyType;
    const valueType = this._def.valueType;
    const pairs = [...ctx.data.entries()].map(([key, value], index) => {
      return {
        key: keyType._parse(new ParseInputLazyPath(ctx, key, ctx.path, [index, "key"])),
        value: valueType._parse(new ParseInputLazyPath(ctx, value, ctx.path, [index, "value"]))
      };
    });
    if (ctx.common.async) {
      const finalMap = /* @__PURE__ */ new Map();
      return Promise.resolve().then(async () => {
        for (const pair of pairs) {
          const key = await pair.key;
          const value = await pair.value;
          if (key.status === "aborted" || value.status === "aborted") {
            return INVALID;
          }
          if (key.status === "dirty" || value.status === "dirty") {
            status.dirty();
          }
          finalMap.set(key.value, value.value);
        }
        return { status: status.value, value: finalMap };
      });
    } else {
      const finalMap = /* @__PURE__ */ new Map();
      for (const pair of pairs) {
        const key = pair.key;
        const value = pair.value;
        if (key.status === "aborted" || value.status === "aborted") {
          return INVALID;
        }
        if (key.status === "dirty" || value.status === "dirty") {
          status.dirty();
        }
        finalMap.set(key.value, value.value);
      }
      return { status: status.value, value: finalMap };
    }
  }
};
ZodMap.create = (keyType, valueType, params) => {
  return new ZodMap({
    valueType,
    keyType,
    typeName: ZodFirstPartyTypeKind.ZodMap,
    ...processCreateParams(params)
  });
};
var ZodSet = class _ZodSet extends ZodType {
  static {
    __name(this, "ZodSet");
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.set) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.set,
        received: ctx.parsedType
      });
      return INVALID;
    }
    const def = this._def;
    if (def.minSize !== null) {
      if (ctx.data.size < def.minSize.value) {
        addIssueToContext(ctx, {
          code: ZodIssueCode.too_small,
          minimum: def.minSize.value,
          type: "set",
          inclusive: true,
          exact: false,
          message: def.minSize.message
        });
        status.dirty();
      }
    }
    if (def.maxSize !== null) {
      if (ctx.data.size > def.maxSize.value) {
        addIssueToContext(ctx, {
          code: ZodIssueCode.too_big,
          maximum: def.maxSize.value,
          type: "set",
          inclusive: true,
          exact: false,
          message: def.maxSize.message
        });
        status.dirty();
      }
    }
    const valueType = this._def.valueType;
    function finalizeSet(elements2) {
      const parsedSet = /* @__PURE__ */ new Set();
      for (const element of elements2) {
        if (element.status === "aborted")
          return INVALID;
        if (element.status === "dirty")
          status.dirty();
        parsedSet.add(element.value);
      }
      return { status: status.value, value: parsedSet };
    }
    __name(finalizeSet, "finalizeSet");
    const elements = [...ctx.data.values()].map((item, i) => valueType._parse(new ParseInputLazyPath(ctx, item, ctx.path, i)));
    if (ctx.common.async) {
      return Promise.all(elements).then((elements2) => finalizeSet(elements2));
    } else {
      return finalizeSet(elements);
    }
  }
  min(minSize, message) {
    return new _ZodSet({
      ...this._def,
      minSize: { value: minSize, message: errorUtil.toString(message) }
    });
  }
  max(maxSize, message) {
    return new _ZodSet({
      ...this._def,
      maxSize: { value: maxSize, message: errorUtil.toString(message) }
    });
  }
  size(size, message) {
    return this.min(size, message).max(size, message);
  }
  nonempty(message) {
    return this.min(1, message);
  }
};
ZodSet.create = (valueType, params) => {
  return new ZodSet({
    valueType,
    minSize: null,
    maxSize: null,
    typeName: ZodFirstPartyTypeKind.ZodSet,
    ...processCreateParams(params)
  });
};
var ZodFunction = class _ZodFunction extends ZodType {
  static {
    __name(this, "ZodFunction");
  }
  constructor() {
    super(...arguments);
    this.validate = this.implement;
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.function) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.function,
        received: ctx.parsedType
      });
      return INVALID;
    }
    function makeArgsIssue(args, error3) {
      return makeIssue({
        data: args,
        path: ctx.path,
        errorMaps: [
          ctx.common.contextualErrorMap,
          ctx.schemaErrorMap,
          getErrorMap(),
          errorMap
        ].filter((x) => !!x),
        issueData: {
          code: ZodIssueCode.invalid_arguments,
          argumentsError: error3
        }
      });
    }
    __name(makeArgsIssue, "makeArgsIssue");
    function makeReturnsIssue(returns, error3) {
      return makeIssue({
        data: returns,
        path: ctx.path,
        errorMaps: [
          ctx.common.contextualErrorMap,
          ctx.schemaErrorMap,
          getErrorMap(),
          errorMap
        ].filter((x) => !!x),
        issueData: {
          code: ZodIssueCode.invalid_return_type,
          returnTypeError: error3
        }
      });
    }
    __name(makeReturnsIssue, "makeReturnsIssue");
    const params = { errorMap: ctx.common.contextualErrorMap };
    const fn = ctx.data;
    if (this._def.returns instanceof ZodPromise) {
      const me = this;
      return OK(async function(...args) {
        const error3 = new ZodError([]);
        const parsedArgs = await me._def.args.parseAsync(args, params).catch((e) => {
          error3.addIssue(makeArgsIssue(args, e));
          throw error3;
        });
        const result = await Reflect.apply(fn, this, parsedArgs);
        const parsedReturns = await me._def.returns._def.type.parseAsync(result, params).catch((e) => {
          error3.addIssue(makeReturnsIssue(result, e));
          throw error3;
        });
        return parsedReturns;
      });
    } else {
      const me = this;
      return OK(function(...args) {
        const parsedArgs = me._def.args.safeParse(args, params);
        if (!parsedArgs.success) {
          throw new ZodError([makeArgsIssue(args, parsedArgs.error)]);
        }
        const result = Reflect.apply(fn, this, parsedArgs.data);
        const parsedReturns = me._def.returns.safeParse(result, params);
        if (!parsedReturns.success) {
          throw new ZodError([makeReturnsIssue(result, parsedReturns.error)]);
        }
        return parsedReturns.data;
      });
    }
  }
  parameters() {
    return this._def.args;
  }
  returnType() {
    return this._def.returns;
  }
  args(...items) {
    return new _ZodFunction({
      ...this._def,
      args: ZodTuple.create(items).rest(ZodUnknown.create())
    });
  }
  returns(returnType) {
    return new _ZodFunction({
      ...this._def,
      returns: returnType
    });
  }
  implement(func) {
    const validatedFunc = this.parse(func);
    return validatedFunc;
  }
  strictImplement(func) {
    const validatedFunc = this.parse(func);
    return validatedFunc;
  }
  static create(args, returns, params) {
    return new _ZodFunction({
      args: args ? args : ZodTuple.create([]).rest(ZodUnknown.create()),
      returns: returns || ZodUnknown.create(),
      typeName: ZodFirstPartyTypeKind.ZodFunction,
      ...processCreateParams(params)
    });
  }
};
var ZodLazy = class extends ZodType {
  static {
    __name(this, "ZodLazy");
  }
  get schema() {
    return this._def.getter();
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    const lazySchema = this._def.getter();
    return lazySchema._parse({ data: ctx.data, path: ctx.path, parent: ctx });
  }
};
ZodLazy.create = (getter, params) => {
  return new ZodLazy({
    getter,
    typeName: ZodFirstPartyTypeKind.ZodLazy,
    ...processCreateParams(params)
  });
};
var ZodLiteral = class extends ZodType {
  static {
    __name(this, "ZodLiteral");
  }
  _parse(input) {
    if (input.data !== this._def.value) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        received: ctx.data,
        code: ZodIssueCode.invalid_literal,
        expected: this._def.value
      });
      return INVALID;
    }
    return { status: "valid", value: input.data };
  }
  get value() {
    return this._def.value;
  }
};
ZodLiteral.create = (value, params) => {
  return new ZodLiteral({
    value,
    typeName: ZodFirstPartyTypeKind.ZodLiteral,
    ...processCreateParams(params)
  });
};
function createZodEnum(values, params) {
  return new ZodEnum({
    values,
    typeName: ZodFirstPartyTypeKind.ZodEnum,
    ...processCreateParams(params)
  });
}
__name(createZodEnum, "createZodEnum");
var ZodEnum = class _ZodEnum extends ZodType {
  static {
    __name(this, "ZodEnum");
  }
  constructor() {
    super(...arguments);
    _ZodEnum_cache.set(this, void 0);
  }
  _parse(input) {
    if (typeof input.data !== "string") {
      const ctx = this._getOrReturnCtx(input);
      const expectedValues = this._def.values;
      addIssueToContext(ctx, {
        expected: util.joinValues(expectedValues),
        received: ctx.parsedType,
        code: ZodIssueCode.invalid_type
      });
      return INVALID;
    }
    if (!__classPrivateFieldGet(this, _ZodEnum_cache, "f")) {
      __classPrivateFieldSet(this, _ZodEnum_cache, new Set(this._def.values), "f");
    }
    if (!__classPrivateFieldGet(this, _ZodEnum_cache, "f").has(input.data)) {
      const ctx = this._getOrReturnCtx(input);
      const expectedValues = this._def.values;
      addIssueToContext(ctx, {
        received: ctx.data,
        code: ZodIssueCode.invalid_enum_value,
        options: expectedValues
      });
      return INVALID;
    }
    return OK(input.data);
  }
  get options() {
    return this._def.values;
  }
  get enum() {
    const enumValues = {};
    for (const val of this._def.values) {
      enumValues[val] = val;
    }
    return enumValues;
  }
  get Values() {
    const enumValues = {};
    for (const val of this._def.values) {
      enumValues[val] = val;
    }
    return enumValues;
  }
  get Enum() {
    const enumValues = {};
    for (const val of this._def.values) {
      enumValues[val] = val;
    }
    return enumValues;
  }
  extract(values, newDef = this._def) {
    return _ZodEnum.create(values, {
      ...this._def,
      ...newDef
    });
  }
  exclude(values, newDef = this._def) {
    return _ZodEnum.create(this.options.filter((opt) => !values.includes(opt)), {
      ...this._def,
      ...newDef
    });
  }
};
_ZodEnum_cache = /* @__PURE__ */ new WeakMap();
ZodEnum.create = createZodEnum;
var ZodNativeEnum = class extends ZodType {
  static {
    __name(this, "ZodNativeEnum");
  }
  constructor() {
    super(...arguments);
    _ZodNativeEnum_cache.set(this, void 0);
  }
  _parse(input) {
    const nativeEnumValues = util.getValidEnumValues(this._def.values);
    const ctx = this._getOrReturnCtx(input);
    if (ctx.parsedType !== ZodParsedType.string && ctx.parsedType !== ZodParsedType.number) {
      const expectedValues = util.objectValues(nativeEnumValues);
      addIssueToContext(ctx, {
        expected: util.joinValues(expectedValues),
        received: ctx.parsedType,
        code: ZodIssueCode.invalid_type
      });
      return INVALID;
    }
    if (!__classPrivateFieldGet(this, _ZodNativeEnum_cache, "f")) {
      __classPrivateFieldSet(this, _ZodNativeEnum_cache, new Set(util.getValidEnumValues(this._def.values)), "f");
    }
    if (!__classPrivateFieldGet(this, _ZodNativeEnum_cache, "f").has(input.data)) {
      const expectedValues = util.objectValues(nativeEnumValues);
      addIssueToContext(ctx, {
        received: ctx.data,
        code: ZodIssueCode.invalid_enum_value,
        options: expectedValues
      });
      return INVALID;
    }
    return OK(input.data);
  }
  get enum() {
    return this._def.values;
  }
};
_ZodNativeEnum_cache = /* @__PURE__ */ new WeakMap();
ZodNativeEnum.create = (values, params) => {
  return new ZodNativeEnum({
    values,
    typeName: ZodFirstPartyTypeKind.ZodNativeEnum,
    ...processCreateParams(params)
  });
};
var ZodPromise = class extends ZodType {
  static {
    __name(this, "ZodPromise");
  }
  unwrap() {
    return this._def.type;
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    if (ctx.parsedType !== ZodParsedType.promise && ctx.common.async === false) {
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.promise,
        received: ctx.parsedType
      });
      return INVALID;
    }
    const promisified = ctx.parsedType === ZodParsedType.promise ? ctx.data : Promise.resolve(ctx.data);
    return OK(promisified.then((data) => {
      return this._def.type.parseAsync(data, {
        path: ctx.path,
        errorMap: ctx.common.contextualErrorMap
      });
    }));
  }
};
ZodPromise.create = (schema, params) => {
  return new ZodPromise({
    type: schema,
    typeName: ZodFirstPartyTypeKind.ZodPromise,
    ...processCreateParams(params)
  });
};
var ZodEffects = class extends ZodType {
  static {
    __name(this, "ZodEffects");
  }
  innerType() {
    return this._def.schema;
  }
  sourceType() {
    return this._def.schema._def.typeName === ZodFirstPartyTypeKind.ZodEffects ? this._def.schema.sourceType() : this._def.schema;
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    const effect = this._def.effect || null;
    const checkCtx = {
      addIssue: /* @__PURE__ */ __name((arg) => {
        addIssueToContext(ctx, arg);
        if (arg.fatal) {
          status.abort();
        } else {
          status.dirty();
        }
      }, "addIssue"),
      get path() {
        return ctx.path;
      }
    };
    checkCtx.addIssue = checkCtx.addIssue.bind(checkCtx);
    if (effect.type === "preprocess") {
      const processed = effect.transform(ctx.data, checkCtx);
      if (ctx.common.async) {
        return Promise.resolve(processed).then(async (processed2) => {
          if (status.value === "aborted")
            return INVALID;
          const result = await this._def.schema._parseAsync({
            data: processed2,
            path: ctx.path,
            parent: ctx
          });
          if (result.status === "aborted")
            return INVALID;
          if (result.status === "dirty")
            return DIRTY(result.value);
          if (status.value === "dirty")
            return DIRTY(result.value);
          return result;
        });
      } else {
        if (status.value === "aborted")
          return INVALID;
        const result = this._def.schema._parseSync({
          data: processed,
          path: ctx.path,
          parent: ctx
        });
        if (result.status === "aborted")
          return INVALID;
        if (result.status === "dirty")
          return DIRTY(result.value);
        if (status.value === "dirty")
          return DIRTY(result.value);
        return result;
      }
    }
    if (effect.type === "refinement") {
      const executeRefinement = /* @__PURE__ */ __name((acc) => {
        const result = effect.refinement(acc, checkCtx);
        if (ctx.common.async) {
          return Promise.resolve(result);
        }
        if (result instanceof Promise) {
          throw new Error("Async refinement encountered during synchronous parse operation. Use .parseAsync instead.");
        }
        return acc;
      }, "executeRefinement");
      if (ctx.common.async === false) {
        const inner = this._def.schema._parseSync({
          data: ctx.data,
          path: ctx.path,
          parent: ctx
        });
        if (inner.status === "aborted")
          return INVALID;
        if (inner.status === "dirty")
          status.dirty();
        executeRefinement(inner.value);
        return { status: status.value, value: inner.value };
      } else {
        return this._def.schema._parseAsync({ data: ctx.data, path: ctx.path, parent: ctx }).then((inner) => {
          if (inner.status === "aborted")
            return INVALID;
          if (inner.status === "dirty")
            status.dirty();
          return executeRefinement(inner.value).then(() => {
            return { status: status.value, value: inner.value };
          });
        });
      }
    }
    if (effect.type === "transform") {
      if (ctx.common.async === false) {
        const base = this._def.schema._parseSync({
          data: ctx.data,
          path: ctx.path,
          parent: ctx
        });
        if (!isValid(base))
          return base;
        const result = effect.transform(base.value, checkCtx);
        if (result instanceof Promise) {
          throw new Error(`Asynchronous transform encountered during synchronous parse operation. Use .parseAsync instead.`);
        }
        return { status: status.value, value: result };
      } else {
        return this._def.schema._parseAsync({ data: ctx.data, path: ctx.path, parent: ctx }).then((base) => {
          if (!isValid(base))
            return base;
          return Promise.resolve(effect.transform(base.value, checkCtx)).then((result) => ({ status: status.value, value: result }));
        });
      }
    }
    util.assertNever(effect);
  }
};
ZodEffects.create = (schema, effect, params) => {
  return new ZodEffects({
    schema,
    typeName: ZodFirstPartyTypeKind.ZodEffects,
    effect,
    ...processCreateParams(params)
  });
};
ZodEffects.createWithPreprocess = (preprocess, schema, params) => {
  return new ZodEffects({
    schema,
    effect: { type: "preprocess", transform: preprocess },
    typeName: ZodFirstPartyTypeKind.ZodEffects,
    ...processCreateParams(params)
  });
};
var ZodOptional = class extends ZodType {
  static {
    __name(this, "ZodOptional");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType === ZodParsedType.undefined) {
      return OK(void 0);
    }
    return this._def.innerType._parse(input);
  }
  unwrap() {
    return this._def.innerType;
  }
};
ZodOptional.create = (type, params) => {
  return new ZodOptional({
    innerType: type,
    typeName: ZodFirstPartyTypeKind.ZodOptional,
    ...processCreateParams(params)
  });
};
var ZodNullable = class extends ZodType {
  static {
    __name(this, "ZodNullable");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType === ZodParsedType.null) {
      return OK(null);
    }
    return this._def.innerType._parse(input);
  }
  unwrap() {
    return this._def.innerType;
  }
};
ZodNullable.create = (type, params) => {
  return new ZodNullable({
    innerType: type,
    typeName: ZodFirstPartyTypeKind.ZodNullable,
    ...processCreateParams(params)
  });
};
var ZodDefault = class extends ZodType {
  static {
    __name(this, "ZodDefault");
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    let data = ctx.data;
    if (ctx.parsedType === ZodParsedType.undefined) {
      data = this._def.defaultValue();
    }
    return this._def.innerType._parse({
      data,
      path: ctx.path,
      parent: ctx
    });
  }
  removeDefault() {
    return this._def.innerType;
  }
};
ZodDefault.create = (type, params) => {
  return new ZodDefault({
    innerType: type,
    typeName: ZodFirstPartyTypeKind.ZodDefault,
    defaultValue: typeof params.default === "function" ? params.default : () => params.default,
    ...processCreateParams(params)
  });
};
var ZodCatch = class extends ZodType {
  static {
    __name(this, "ZodCatch");
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    const newCtx = {
      ...ctx,
      common: {
        ...ctx.common,
        issues: []
      }
    };
    const result = this._def.innerType._parse({
      data: newCtx.data,
      path: newCtx.path,
      parent: {
        ...newCtx
      }
    });
    if (isAsync(result)) {
      return result.then((result2) => {
        return {
          status: "valid",
          value: result2.status === "valid" ? result2.value : this._def.catchValue({
            get error() {
              return new ZodError(newCtx.common.issues);
            },
            input: newCtx.data
          })
        };
      });
    } else {
      return {
        status: "valid",
        value: result.status === "valid" ? result.value : this._def.catchValue({
          get error() {
            return new ZodError(newCtx.common.issues);
          },
          input: newCtx.data
        })
      };
    }
  }
  removeCatch() {
    return this._def.innerType;
  }
};
ZodCatch.create = (type, params) => {
  return new ZodCatch({
    innerType: type,
    typeName: ZodFirstPartyTypeKind.ZodCatch,
    catchValue: typeof params.catch === "function" ? params.catch : () => params.catch,
    ...processCreateParams(params)
  });
};
var ZodNaN = class extends ZodType {
  static {
    __name(this, "ZodNaN");
  }
  _parse(input) {
    const parsedType = this._getType(input);
    if (parsedType !== ZodParsedType.nan) {
      const ctx = this._getOrReturnCtx(input);
      addIssueToContext(ctx, {
        code: ZodIssueCode.invalid_type,
        expected: ZodParsedType.nan,
        received: ctx.parsedType
      });
      return INVALID;
    }
    return { status: "valid", value: input.data };
  }
};
ZodNaN.create = (params) => {
  return new ZodNaN({
    typeName: ZodFirstPartyTypeKind.ZodNaN,
    ...processCreateParams(params)
  });
};
var BRAND = Symbol("zod_brand");
var ZodBranded = class extends ZodType {
  static {
    __name(this, "ZodBranded");
  }
  _parse(input) {
    const { ctx } = this._processInputParams(input);
    const data = ctx.data;
    return this._def.type._parse({
      data,
      path: ctx.path,
      parent: ctx
    });
  }
  unwrap() {
    return this._def.type;
  }
};
var ZodPipeline = class _ZodPipeline extends ZodType {
  static {
    __name(this, "ZodPipeline");
  }
  _parse(input) {
    const { status, ctx } = this._processInputParams(input);
    if (ctx.common.async) {
      const handleAsync = /* @__PURE__ */ __name(async () => {
        const inResult = await this._def.in._parseAsync({
          data: ctx.data,
          path: ctx.path,
          parent: ctx
        });
        if (inResult.status === "aborted")
          return INVALID;
        if (inResult.status === "dirty") {
          status.dirty();
          return DIRTY(inResult.value);
        } else {
          return this._def.out._parseAsync({
            data: inResult.value,
            path: ctx.path,
            parent: ctx
          });
        }
      }, "handleAsync");
      return handleAsync();
    } else {
      const inResult = this._def.in._parseSync({
        data: ctx.data,
        path: ctx.path,
        parent: ctx
      });
      if (inResult.status === "aborted")
        return INVALID;
      if (inResult.status === "dirty") {
        status.dirty();
        return {
          status: "dirty",
          value: inResult.value
        };
      } else {
        return this._def.out._parseSync({
          data: inResult.value,
          path: ctx.path,
          parent: ctx
        });
      }
    }
  }
  static create(a, b) {
    return new _ZodPipeline({
      in: a,
      out: b,
      typeName: ZodFirstPartyTypeKind.ZodPipeline
    });
  }
};
var ZodReadonly = class extends ZodType {
  static {
    __name(this, "ZodReadonly");
  }
  _parse(input) {
    const result = this._def.innerType._parse(input);
    const freeze = /* @__PURE__ */ __name((data) => {
      if (isValid(data)) {
        data.value = Object.freeze(data.value);
      }
      return data;
    }, "freeze");
    return isAsync(result) ? result.then((data) => freeze(data)) : freeze(result);
  }
  unwrap() {
    return this._def.innerType;
  }
};
ZodReadonly.create = (type, params) => {
  return new ZodReadonly({
    innerType: type,
    typeName: ZodFirstPartyTypeKind.ZodReadonly,
    ...processCreateParams(params)
  });
};
function cleanParams(params, data) {
  const p = typeof params === "function" ? params(data) : typeof params === "string" ? { message: params } : params;
  const p2 = typeof p === "string" ? { message: p } : p;
  return p2;
}
__name(cleanParams, "cleanParams");
function custom(check, _params = {}, fatal) {
  if (check)
    return ZodAny.create().superRefine((data, ctx) => {
      var _a, _b;
      const r = check(data);
      if (r instanceof Promise) {
        return r.then((r2) => {
          var _a2, _b2;
          if (!r2) {
            const params = cleanParams(_params, data);
            const _fatal = (_b2 = (_a2 = params.fatal) !== null && _a2 !== void 0 ? _a2 : fatal) !== null && _b2 !== void 0 ? _b2 : true;
            ctx.addIssue({ code: "custom", ...params, fatal: _fatal });
          }
        });
      }
      if (!r) {
        const params = cleanParams(_params, data);
        const _fatal = (_b = (_a = params.fatal) !== null && _a !== void 0 ? _a : fatal) !== null && _b !== void 0 ? _b : true;
        ctx.addIssue({ code: "custom", ...params, fatal: _fatal });
      }
      return;
    });
  return ZodAny.create();
}
__name(custom, "custom");
var late = {
  object: ZodObject.lazycreate
};
var ZodFirstPartyTypeKind;
(function(ZodFirstPartyTypeKind2) {
  ZodFirstPartyTypeKind2["ZodString"] = "ZodString";
  ZodFirstPartyTypeKind2["ZodNumber"] = "ZodNumber";
  ZodFirstPartyTypeKind2["ZodNaN"] = "ZodNaN";
  ZodFirstPartyTypeKind2["ZodBigInt"] = "ZodBigInt";
  ZodFirstPartyTypeKind2["ZodBoolean"] = "ZodBoolean";
  ZodFirstPartyTypeKind2["ZodDate"] = "ZodDate";
  ZodFirstPartyTypeKind2["ZodSymbol"] = "ZodSymbol";
  ZodFirstPartyTypeKind2["ZodUndefined"] = "ZodUndefined";
  ZodFirstPartyTypeKind2["ZodNull"] = "ZodNull";
  ZodFirstPartyTypeKind2["ZodAny"] = "ZodAny";
  ZodFirstPartyTypeKind2["ZodUnknown"] = "ZodUnknown";
  ZodFirstPartyTypeKind2["ZodNever"] = "ZodNever";
  ZodFirstPartyTypeKind2["ZodVoid"] = "ZodVoid";
  ZodFirstPartyTypeKind2["ZodArray"] = "ZodArray";
  ZodFirstPartyTypeKind2["ZodObject"] = "ZodObject";
  ZodFirstPartyTypeKind2["ZodUnion"] = "ZodUnion";
  ZodFirstPartyTypeKind2["ZodDiscriminatedUnion"] = "ZodDiscriminatedUnion";
  ZodFirstPartyTypeKind2["ZodIntersection"] = "ZodIntersection";
  ZodFirstPartyTypeKind2["ZodTuple"] = "ZodTuple";
  ZodFirstPartyTypeKind2["ZodRecord"] = "ZodRecord";
  ZodFirstPartyTypeKind2["ZodMap"] = "ZodMap";
  ZodFirstPartyTypeKind2["ZodSet"] = "ZodSet";
  ZodFirstPartyTypeKind2["ZodFunction"] = "ZodFunction";
  ZodFirstPartyTypeKind2["ZodLazy"] = "ZodLazy";
  ZodFirstPartyTypeKind2["ZodLiteral"] = "ZodLiteral";
  ZodFirstPartyTypeKind2["ZodEnum"] = "ZodEnum";
  ZodFirstPartyTypeKind2["ZodEffects"] = "ZodEffects";
  ZodFirstPartyTypeKind2["ZodNativeEnum"] = "ZodNativeEnum";
  ZodFirstPartyTypeKind2["ZodOptional"] = "ZodOptional";
  ZodFirstPartyTypeKind2["ZodNullable"] = "ZodNullable";
  ZodFirstPartyTypeKind2["ZodDefault"] = "ZodDefault";
  ZodFirstPartyTypeKind2["ZodCatch"] = "ZodCatch";
  ZodFirstPartyTypeKind2["ZodPromise"] = "ZodPromise";
  ZodFirstPartyTypeKind2["ZodBranded"] = "ZodBranded";
  ZodFirstPartyTypeKind2["ZodPipeline"] = "ZodPipeline";
  ZodFirstPartyTypeKind2["ZodReadonly"] = "ZodReadonly";
})(ZodFirstPartyTypeKind || (ZodFirstPartyTypeKind = {}));
var instanceOfType = /* @__PURE__ */ __name((cls, params = {
  message: `Input not instance of ${cls.name}`
}) => custom((data) => data instanceof cls, params), "instanceOfType");
var stringType = ZodString.create;
var numberType = ZodNumber.create;
var nanType = ZodNaN.create;
var bigIntType = ZodBigInt.create;
var booleanType = ZodBoolean.create;
var dateType = ZodDate.create;
var symbolType = ZodSymbol.create;
var undefinedType = ZodUndefined.create;
var nullType = ZodNull.create;
var anyType = ZodAny.create;
var unknownType = ZodUnknown.create;
var neverType = ZodNever.create;
var voidType = ZodVoid.create;
var arrayType = ZodArray.create;
var objectType = ZodObject.create;
var strictObjectType = ZodObject.strictCreate;
var unionType = ZodUnion.create;
var discriminatedUnionType = ZodDiscriminatedUnion.create;
var intersectionType = ZodIntersection.create;
var tupleType = ZodTuple.create;
var recordType = ZodRecord.create;
var mapType = ZodMap.create;
var setType = ZodSet.create;
var functionType = ZodFunction.create;
var lazyType = ZodLazy.create;
var literalType = ZodLiteral.create;
var enumType = ZodEnum.create;
var nativeEnumType = ZodNativeEnum.create;
var promiseType = ZodPromise.create;
var effectsType = ZodEffects.create;
var optionalType = ZodOptional.create;
var nullableType = ZodNullable.create;
var preprocessType = ZodEffects.createWithPreprocess;
var pipelineType = ZodPipeline.create;
var ostring = /* @__PURE__ */ __name(() => stringType().optional(), "ostring");
var onumber = /* @__PURE__ */ __name(() => numberType().optional(), "onumber");
var oboolean = /* @__PURE__ */ __name(() => booleanType().optional(), "oboolean");
var coerce = {
  string: /* @__PURE__ */ __name((arg) => ZodString.create({ ...arg, coerce: true }), "string"),
  number: /* @__PURE__ */ __name((arg) => ZodNumber.create({ ...arg, coerce: true }), "number"),
  boolean: /* @__PURE__ */ __name((arg) => ZodBoolean.create({
    ...arg,
    coerce: true
  }), "boolean"),
  bigint: /* @__PURE__ */ __name((arg) => ZodBigInt.create({ ...arg, coerce: true }), "bigint"),
  date: /* @__PURE__ */ __name((arg) => ZodDate.create({ ...arg, coerce: true }), "date")
};
var NEVER = INVALID;
var z = /* @__PURE__ */ Object.freeze({
  __proto__: null,
  defaultErrorMap: errorMap,
  setErrorMap,
  getErrorMap,
  makeIssue,
  EMPTY_PATH,
  addIssueToContext,
  ParseStatus,
  INVALID,
  DIRTY,
  OK,
  isAborted,
  isDirty,
  isValid,
  isAsync,
  get util() {
    return util;
  },
  get objectUtil() {
    return objectUtil;
  },
  ZodParsedType,
  getParsedType,
  ZodType,
  datetimeRegex,
  ZodString,
  ZodNumber,
  ZodBigInt,
  ZodBoolean,
  ZodDate,
  ZodSymbol,
  ZodUndefined,
  ZodNull,
  ZodAny,
  ZodUnknown,
  ZodNever,
  ZodVoid,
  ZodArray,
  ZodObject,
  ZodUnion,
  ZodDiscriminatedUnion,
  ZodIntersection,
  ZodTuple,
  ZodRecord,
  ZodMap,
  ZodSet,
  ZodFunction,
  ZodLazy,
  ZodLiteral,
  ZodEnum,
  ZodNativeEnum,
  ZodPromise,
  ZodEffects,
  ZodTransformer: ZodEffects,
  ZodOptional,
  ZodNullable,
  ZodDefault,
  ZodCatch,
  ZodNaN,
  BRAND,
  ZodBranded,
  ZodPipeline,
  ZodReadonly,
  custom,
  Schema: ZodType,
  ZodSchema: ZodType,
  late,
  get ZodFirstPartyTypeKind() {
    return ZodFirstPartyTypeKind;
  },
  coerce,
  any: anyType,
  array: arrayType,
  bigint: bigIntType,
  boolean: booleanType,
  date: dateType,
  discriminatedUnion: discriminatedUnionType,
  effect: effectsType,
  "enum": enumType,
  "function": functionType,
  "instanceof": instanceOfType,
  intersection: intersectionType,
  lazy: lazyType,
  literal: literalType,
  map: mapType,
  nan: nanType,
  nativeEnum: nativeEnumType,
  never: neverType,
  "null": nullType,
  nullable: nullableType,
  number: numberType,
  object: objectType,
  oboolean,
  onumber,
  optional: optionalType,
  ostring,
  pipeline: pipelineType,
  preprocess: preprocessType,
  promise: promiseType,
  record: recordType,
  set: setType,
  strictObject: strictObjectType,
  string: stringType,
  symbol: symbolType,
  transformer: effectsType,
  tuple: tupleType,
  "undefined": undefinedType,
  union: unionType,
  unknown: unknownType,
  "void": voidType,
  NEVER,
  ZodIssueCode,
  quotelessJson,
  ZodError
});

// node_modules/@modelcontextprotocol/sdk/dist/esm/types.js
var LATEST_PROTOCOL_VERSION = "2024-11-05";
var SUPPORTED_PROTOCOL_VERSIONS = [
  LATEST_PROTOCOL_VERSION,
  "2024-10-07"
];
var JSONRPC_VERSION = "2.0";
var ProgressTokenSchema = z.union([z.string(), z.number().int()]);
var CursorSchema = z.string();
var BaseRequestParamsSchema = z.object({
  _meta: z.optional(z.object({
    /**
     * If specified, the caller is requesting out-of-band progress notifications for this request (as represented by notifications/progress). The value of this parameter is an opaque token that will be attached to any subsequent notifications. The receiver is not obligated to provide these notifications.
     */
    progressToken: z.optional(ProgressTokenSchema)
  }).passthrough())
}).passthrough();
var RequestSchema = z.object({
  method: z.string(),
  params: z.optional(BaseRequestParamsSchema)
});
var BaseNotificationParamsSchema = z.object({
  /**
   * This parameter name is reserved by MCP to allow clients and servers to attach additional metadata to their notifications.
   */
  _meta: z.optional(z.object({}).passthrough())
}).passthrough();
var NotificationSchema = z.object({
  method: z.string(),
  params: z.optional(BaseNotificationParamsSchema)
});
var ResultSchema = z.object({
  /**
   * This result property is reserved by the protocol to allow clients and servers to attach additional metadata to their responses.
   */
  _meta: z.optional(z.object({}).passthrough())
}).passthrough();
var RequestIdSchema = z.union([z.string(), z.number().int()]);
var JSONRPCRequestSchema = z.object({
  jsonrpc: z.literal(JSONRPC_VERSION),
  id: RequestIdSchema
}).merge(RequestSchema).strict();
var isJSONRPCRequest = /* @__PURE__ */ __name((value) => JSONRPCRequestSchema.safeParse(value).success, "isJSONRPCRequest");
var JSONRPCNotificationSchema = z.object({
  jsonrpc: z.literal(JSONRPC_VERSION)
}).merge(NotificationSchema).strict();
var isJSONRPCNotification = /* @__PURE__ */ __name((value) => JSONRPCNotificationSchema.safeParse(value).success, "isJSONRPCNotification");
var JSONRPCResponseSchema = z.object({
  jsonrpc: z.literal(JSONRPC_VERSION),
  id: RequestIdSchema,
  result: ResultSchema
}).strict();
var isJSONRPCResponse = /* @__PURE__ */ __name((value) => JSONRPCResponseSchema.safeParse(value).success, "isJSONRPCResponse");
var ErrorCode;
(function(ErrorCode2) {
  ErrorCode2[ErrorCode2["ConnectionClosed"] = -32e3] = "ConnectionClosed";
  ErrorCode2[ErrorCode2["RequestTimeout"] = -32001] = "RequestTimeout";
  ErrorCode2[ErrorCode2["ParseError"] = -32700] = "ParseError";
  ErrorCode2[ErrorCode2["InvalidRequest"] = -32600] = "InvalidRequest";
  ErrorCode2[ErrorCode2["MethodNotFound"] = -32601] = "MethodNotFound";
  ErrorCode2[ErrorCode2["InvalidParams"] = -32602] = "InvalidParams";
  ErrorCode2[ErrorCode2["InternalError"] = -32603] = "InternalError";
})(ErrorCode || (ErrorCode = {}));
var JSONRPCErrorSchema = z.object({
  jsonrpc: z.literal(JSONRPC_VERSION),
  id: RequestIdSchema,
  error: z.object({
    /**
     * The error type that occurred.
     */
    code: z.number().int(),
    /**
     * A short description of the error. The message SHOULD be limited to a concise single sentence.
     */
    message: z.string(),
    /**
     * Additional information about the error. The value of this member is defined by the sender (e.g. detailed error information, nested errors etc.).
     */
    data: z.optional(z.unknown())
  })
}).strict();
var isJSONRPCError = /* @__PURE__ */ __name((value) => JSONRPCErrorSchema.safeParse(value).success, "isJSONRPCError");
var JSONRPCMessageSchema = z.union([
  JSONRPCRequestSchema,
  JSONRPCNotificationSchema,
  JSONRPCResponseSchema,
  JSONRPCErrorSchema
]);
var EmptyResultSchema = ResultSchema.strict();
var CancelledNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/cancelled"),
  params: BaseNotificationParamsSchema.extend({
    /**
     * The ID of the request to cancel.
     *
     * This MUST correspond to the ID of a request previously issued in the same direction.
     */
    requestId: RequestIdSchema,
    /**
     * An optional string describing the reason for the cancellation. This MAY be logged or presented to the user.
     */
    reason: z.string().optional()
  })
});
var ImplementationSchema = z.object({
  name: z.string(),
  version: z.string()
}).passthrough();
var ClientCapabilitiesSchema = z.object({
  /**
   * Experimental, non-standard capabilities that the client supports.
   */
  experimental: z.optional(z.object({}).passthrough()),
  /**
   * Present if the client supports sampling from an LLM.
   */
  sampling: z.optional(z.object({}).passthrough()),
  /**
   * Present if the client supports listing roots.
   */
  roots: z.optional(z.object({
    /**
     * Whether the client supports issuing notifications for changes to the roots list.
     */
    listChanged: z.optional(z.boolean())
  }).passthrough())
}).passthrough();
var InitializeRequestSchema = RequestSchema.extend({
  method: z.literal("initialize"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The latest version of the Model Context Protocol that the client supports. The client MAY decide to support older versions as well.
     */
    protocolVersion: z.string(),
    capabilities: ClientCapabilitiesSchema,
    clientInfo: ImplementationSchema
  })
});
var ServerCapabilitiesSchema = z.object({
  /**
   * Experimental, non-standard capabilities that the server supports.
   */
  experimental: z.optional(z.object({}).passthrough()),
  /**
   * Present if the server supports sending log messages to the client.
   */
  logging: z.optional(z.object({}).passthrough()),
  /**
   * Present if the server supports sending completions to the client.
   */
  completions: z.optional(z.object({}).passthrough()),
  /**
   * Present if the server offers any prompt templates.
   */
  prompts: z.optional(z.object({
    /**
     * Whether this server supports issuing notifications for changes to the prompt list.
     */
    listChanged: z.optional(z.boolean())
  }).passthrough()),
  /**
   * Present if the server offers any resources to read.
   */
  resources: z.optional(z.object({
    /**
     * Whether this server supports clients subscribing to resource updates.
     */
    subscribe: z.optional(z.boolean()),
    /**
     * Whether this server supports issuing notifications for changes to the resource list.
     */
    listChanged: z.optional(z.boolean())
  }).passthrough()),
  /**
   * Present if the server offers any tools to call.
   */
  tools: z.optional(z.object({
    /**
     * Whether this server supports issuing notifications for changes to the tool list.
     */
    listChanged: z.optional(z.boolean())
  }).passthrough())
}).passthrough();
var InitializeResultSchema = ResultSchema.extend({
  /**
   * The version of the Model Context Protocol that the server wants to use. This may not match the version that the client requested. If the client cannot support this version, it MUST disconnect.
   */
  protocolVersion: z.string(),
  capabilities: ServerCapabilitiesSchema,
  serverInfo: ImplementationSchema,
  /**
   * Instructions describing how to use the server and its features.
   *
   * This can be used by clients to improve the LLM's understanding of available tools, resources, etc. It can be thought of like a "hint" to the model. For example, this information MAY be added to the system prompt.
   */
  instructions: z.optional(z.string())
});
var InitializedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/initialized")
});
var PingRequestSchema = RequestSchema.extend({
  method: z.literal("ping")
});
var ProgressSchema = z.object({
  /**
   * The progress thus far. This should increase every time progress is made, even if the total is unknown.
   */
  progress: z.number(),
  /**
   * Total number of items to process (or total progress required), if known.
   */
  total: z.optional(z.number())
}).passthrough();
var ProgressNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/progress"),
  params: BaseNotificationParamsSchema.merge(ProgressSchema).extend({
    /**
     * The progress token which was given in the initial request, used to associate this notification with the request that is proceeding.
     */
    progressToken: ProgressTokenSchema
  })
});
var PaginatedRequestSchema = RequestSchema.extend({
  params: BaseRequestParamsSchema.extend({
    /**
     * An opaque token representing the current pagination position.
     * If provided, the server should return results starting after this cursor.
     */
    cursor: z.optional(CursorSchema)
  }).optional()
});
var PaginatedResultSchema = ResultSchema.extend({
  /**
   * An opaque token representing the pagination position after the last returned result.
   * If present, there may be more results available.
   */
  nextCursor: z.optional(CursorSchema)
});
var ResourceContentsSchema = z.object({
  /**
   * The URI of this resource.
   */
  uri: z.string(),
  /**
   * The MIME type of this resource, if known.
   */
  mimeType: z.optional(z.string())
}).passthrough();
var TextResourceContentsSchema = ResourceContentsSchema.extend({
  /**
   * The text of the item. This must only be set if the item can actually be represented as text (not binary data).
   */
  text: z.string()
});
var BlobResourceContentsSchema = ResourceContentsSchema.extend({
  /**
   * A base64-encoded string representing the binary data of the item.
   */
  blob: z.string().base64()
});
var ResourceSchema = z.object({
  /**
   * The URI of this resource.
   */
  uri: z.string(),
  /**
   * A human-readable name for this resource.
   *
   * This can be used by clients to populate UI elements.
   */
  name: z.string(),
  /**
   * A description of what this resource represents.
   *
   * This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
   */
  description: z.optional(z.string()),
  /**
   * The MIME type of this resource, if known.
   */
  mimeType: z.optional(z.string())
}).passthrough();
var ResourceTemplateSchema = z.object({
  /**
   * A URI template (according to RFC 6570) that can be used to construct resource URIs.
   */
  uriTemplate: z.string(),
  /**
   * A human-readable name for the type of resource this template refers to.
   *
   * This can be used by clients to populate UI elements.
   */
  name: z.string(),
  /**
   * A description of what this template is for.
   *
   * This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
   */
  description: z.optional(z.string()),
  /**
   * The MIME type for all resources that match this template. This should only be included if all resources matching this template have the same type.
   */
  mimeType: z.optional(z.string())
}).passthrough();
var ListResourcesRequestSchema = PaginatedRequestSchema.extend({
  method: z.literal("resources/list")
});
var ListResourcesResultSchema = PaginatedResultSchema.extend({
  resources: z.array(ResourceSchema)
});
var ListResourceTemplatesRequestSchema = PaginatedRequestSchema.extend({
  method: z.literal("resources/templates/list")
});
var ListResourceTemplatesResultSchema = PaginatedResultSchema.extend({
  resourceTemplates: z.array(ResourceTemplateSchema)
});
var ReadResourceRequestSchema = RequestSchema.extend({
  method: z.literal("resources/read"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The URI of the resource to read. The URI can use any protocol; it is up to the server how to interpret it.
     */
    uri: z.string()
  })
});
var ReadResourceResultSchema = ResultSchema.extend({
  contents: z.array(z.union([TextResourceContentsSchema, BlobResourceContentsSchema]))
});
var ResourceListChangedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/resources/list_changed")
});
var SubscribeRequestSchema = RequestSchema.extend({
  method: z.literal("resources/subscribe"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The URI of the resource to subscribe to. The URI can use any protocol; it is up to the server how to interpret it.
     */
    uri: z.string()
  })
});
var UnsubscribeRequestSchema = RequestSchema.extend({
  method: z.literal("resources/unsubscribe"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The URI of the resource to unsubscribe from.
     */
    uri: z.string()
  })
});
var ResourceUpdatedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/resources/updated"),
  params: BaseNotificationParamsSchema.extend({
    /**
     * The URI of the resource that has been updated. This might be a sub-resource of the one that the client actually subscribed to.
     */
    uri: z.string()
  })
});
var PromptArgumentSchema = z.object({
  /**
   * The name of the argument.
   */
  name: z.string(),
  /**
   * A human-readable description of the argument.
   */
  description: z.optional(z.string()),
  /**
   * Whether this argument must be provided.
   */
  required: z.optional(z.boolean())
}).passthrough();
var PromptSchema = z.object({
  /**
   * The name of the prompt or prompt template.
   */
  name: z.string(),
  /**
   * An optional description of what this prompt provides
   */
  description: z.optional(z.string()),
  /**
   * A list of arguments to use for templating the prompt.
   */
  arguments: z.optional(z.array(PromptArgumentSchema))
}).passthrough();
var ListPromptsRequestSchema = PaginatedRequestSchema.extend({
  method: z.literal("prompts/list")
});
var ListPromptsResultSchema = PaginatedResultSchema.extend({
  prompts: z.array(PromptSchema)
});
var GetPromptRequestSchema = RequestSchema.extend({
  method: z.literal("prompts/get"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The name of the prompt or prompt template.
     */
    name: z.string(),
    /**
     * Arguments to use for templating the prompt.
     */
    arguments: z.optional(z.record(z.string()))
  })
});
var TextContentSchema = z.object({
  type: z.literal("text"),
  /**
   * The text content of the message.
   */
  text: z.string()
}).passthrough();
var ImageContentSchema = z.object({
  type: z.literal("image"),
  /**
   * The base64-encoded image data.
   */
  data: z.string().base64(),
  /**
   * The MIME type of the image. Different providers may support different image types.
   */
  mimeType: z.string()
}).passthrough();
var AudioContentSchema = z.object({
  type: z.literal("audio"),
  /**
   * The base64-encoded audio data.
   */
  data: z.string().base64(),
  /**
   * The MIME type of the audio. Different providers may support different audio types.
   */
  mimeType: z.string()
}).passthrough();
var EmbeddedResourceSchema = z.object({
  type: z.literal("resource"),
  resource: z.union([TextResourceContentsSchema, BlobResourceContentsSchema])
}).passthrough();
var PromptMessageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.union([
    TextContentSchema,
    ImageContentSchema,
    AudioContentSchema,
    EmbeddedResourceSchema
  ])
}).passthrough();
var GetPromptResultSchema = ResultSchema.extend({
  /**
   * An optional description for the prompt.
   */
  description: z.optional(z.string()),
  messages: z.array(PromptMessageSchema)
});
var PromptListChangedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/prompts/list_changed")
});
var ToolSchema = z.object({
  /**
   * The name of the tool.
   */
  name: z.string(),
  /**
   * A human-readable description of the tool.
   */
  description: z.optional(z.string()),
  /**
   * A JSON Schema object defining the expected parameters for the tool.
   */
  inputSchema: z.object({
    type: z.literal("object"),
    properties: z.optional(z.object({}).passthrough())
  }).passthrough()
}).passthrough();
var ListToolsRequestSchema = PaginatedRequestSchema.extend({
  method: z.literal("tools/list")
});
var ListToolsResultSchema = PaginatedResultSchema.extend({
  tools: z.array(ToolSchema)
});
var CallToolResultSchema = ResultSchema.extend({
  content: z.array(z.union([TextContentSchema, ImageContentSchema, AudioContentSchema, EmbeddedResourceSchema])),
  isError: z.boolean().default(false).optional()
});
var CompatibilityCallToolResultSchema = CallToolResultSchema.or(ResultSchema.extend({
  toolResult: z.unknown()
}));
var CallToolRequestSchema = RequestSchema.extend({
  method: z.literal("tools/call"),
  params: BaseRequestParamsSchema.extend({
    name: z.string(),
    arguments: z.optional(z.record(z.unknown()))
  })
});
var ToolListChangedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/tools/list_changed")
});
var LoggingLevelSchema = z.enum([
  "debug",
  "info",
  "notice",
  "warning",
  "error",
  "critical",
  "alert",
  "emergency"
]);
var SetLevelRequestSchema = RequestSchema.extend({
  method: z.literal("logging/setLevel"),
  params: BaseRequestParamsSchema.extend({
    /**
     * The level of logging that the client wants to receive from the server. The server should send all logs at this level and higher (i.e., more severe) to the client as notifications/logging/message.
     */
    level: LoggingLevelSchema
  })
});
var LoggingMessageNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/message"),
  params: BaseNotificationParamsSchema.extend({
    /**
     * The severity of this log message.
     */
    level: LoggingLevelSchema,
    /**
     * An optional name of the logger issuing this message.
     */
    logger: z.optional(z.string()),
    /**
     * The data to be logged, such as a string message or an object. Any JSON serializable type is allowed here.
     */
    data: z.unknown()
  })
});
var ModelHintSchema = z.object({
  /**
   * A hint for a model name.
   */
  name: z.string().optional()
}).passthrough();
var ModelPreferencesSchema = z.object({
  /**
   * Optional hints to use for model selection.
   */
  hints: z.optional(z.array(ModelHintSchema)),
  /**
   * How much to prioritize cost when selecting a model.
   */
  costPriority: z.optional(z.number().min(0).max(1)),
  /**
   * How much to prioritize sampling speed (latency) when selecting a model.
   */
  speedPriority: z.optional(z.number().min(0).max(1)),
  /**
   * How much to prioritize intelligence and capabilities when selecting a model.
   */
  intelligencePriority: z.optional(z.number().min(0).max(1))
}).passthrough();
var SamplingMessageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.union([TextContentSchema, ImageContentSchema, AudioContentSchema])
}).passthrough();
var CreateMessageRequestSchema = RequestSchema.extend({
  method: z.literal("sampling/createMessage"),
  params: BaseRequestParamsSchema.extend({
    messages: z.array(SamplingMessageSchema),
    /**
     * An optional system prompt the server wants to use for sampling. The client MAY modify or omit this prompt.
     */
    systemPrompt: z.optional(z.string()),
    /**
     * A request to include context from one or more MCP servers (including the caller), to be attached to the prompt. The client MAY ignore this request.
     */
    includeContext: z.optional(z.enum(["none", "thisServer", "allServers"])),
    temperature: z.optional(z.number()),
    /**
     * The maximum number of tokens to sample, as requested by the server. The client MAY choose to sample fewer tokens than requested.
     */
    maxTokens: z.number().int(),
    stopSequences: z.optional(z.array(z.string())),
    /**
     * Optional metadata to pass through to the LLM provider. The format of this metadata is provider-specific.
     */
    metadata: z.optional(z.object({}).passthrough()),
    /**
     * The server's preferences for which model to select.
     */
    modelPreferences: z.optional(ModelPreferencesSchema)
  })
});
var CreateMessageResultSchema = ResultSchema.extend({
  /**
   * The name of the model that generated the message.
   */
  model: z.string(),
  /**
   * The reason why sampling stopped.
   */
  stopReason: z.optional(z.enum(["endTurn", "stopSequence", "maxTokens"]).or(z.string())),
  role: z.enum(["user", "assistant"]),
  content: z.discriminatedUnion("type", [
    TextContentSchema,
    ImageContentSchema,
    AudioContentSchema
  ])
});
var ResourceReferenceSchema = z.object({
  type: z.literal("ref/resource"),
  /**
   * The URI or URI template of the resource.
   */
  uri: z.string()
}).passthrough();
var PromptReferenceSchema = z.object({
  type: z.literal("ref/prompt"),
  /**
   * The name of the prompt or prompt template
   */
  name: z.string()
}).passthrough();
var CompleteRequestSchema = RequestSchema.extend({
  method: z.literal("completion/complete"),
  params: BaseRequestParamsSchema.extend({
    ref: z.union([PromptReferenceSchema, ResourceReferenceSchema]),
    /**
     * The argument's information
     */
    argument: z.object({
      /**
       * The name of the argument
       */
      name: z.string(),
      /**
       * The value of the argument to use for completion matching.
       */
      value: z.string()
    }).passthrough()
  })
});
var CompleteResultSchema = ResultSchema.extend({
  completion: z.object({
    /**
     * An array of completion values. Must not exceed 100 items.
     */
    values: z.array(z.string()).max(100),
    /**
     * The total number of completion options available. This can exceed the number of values actually sent in the response.
     */
    total: z.optional(z.number().int()),
    /**
     * Indicates whether there are additional completion options beyond those provided in the current response, even if the exact total is unknown.
     */
    hasMore: z.optional(z.boolean())
  }).passthrough()
});
var RootSchema = z.object({
  /**
   * The URI identifying the root. This *must* start with file:// for now.
   */
  uri: z.string().startsWith("file://"),
  /**
   * An optional name for the root.
   */
  name: z.optional(z.string())
}).passthrough();
var ListRootsRequestSchema = RequestSchema.extend({
  method: z.literal("roots/list")
});
var ListRootsResultSchema = ResultSchema.extend({
  roots: z.array(RootSchema)
});
var RootsListChangedNotificationSchema = NotificationSchema.extend({
  method: z.literal("notifications/roots/list_changed")
});
var ClientRequestSchema = z.union([
  PingRequestSchema,
  InitializeRequestSchema,
  CompleteRequestSchema,
  SetLevelRequestSchema,
  GetPromptRequestSchema,
  ListPromptsRequestSchema,
  ListResourcesRequestSchema,
  ListResourceTemplatesRequestSchema,
  ReadResourceRequestSchema,
  SubscribeRequestSchema,
  UnsubscribeRequestSchema,
  CallToolRequestSchema,
  ListToolsRequestSchema
]);
var ClientNotificationSchema = z.union([
  CancelledNotificationSchema,
  ProgressNotificationSchema,
  InitializedNotificationSchema,
  RootsListChangedNotificationSchema
]);
var ClientResultSchema = z.union([
  EmptyResultSchema,
  CreateMessageResultSchema,
  ListRootsResultSchema
]);
var ServerRequestSchema = z.union([
  PingRequestSchema,
  CreateMessageRequestSchema,
  ListRootsRequestSchema
]);
var ServerNotificationSchema = z.union([
  CancelledNotificationSchema,
  ProgressNotificationSchema,
  LoggingMessageNotificationSchema,
  ResourceUpdatedNotificationSchema,
  ResourceListChangedNotificationSchema,
  ToolListChangedNotificationSchema,
  PromptListChangedNotificationSchema
]);
var ServerResultSchema = z.union([
  EmptyResultSchema,
  InitializeResultSchema,
  CompleteResultSchema,
  GetPromptResultSchema,
  ListPromptsResultSchema,
  ListResourcesResultSchema,
  ListResourceTemplatesResultSchema,
  ReadResourceResultSchema,
  CallToolResultSchema,
  ListToolsResultSchema
]);
var McpError = class extends Error {
  static {
    __name(this, "McpError");
  }
  constructor(code, message, data) {
    super(`MCP error ${code}: ${message}`);
    this.code = code;
    this.data = data;
    this.name = "McpError";
  }
};

// node_modules/@modelcontextprotocol/sdk/dist/esm/shared/protocol.js
var DEFAULT_REQUEST_TIMEOUT_MSEC = 6e4;
var Protocol = class {
  static {
    __name(this, "Protocol");
  }
  constructor(_options) {
    this._options = _options;
    this._requestMessageId = 0;
    this._requestHandlers = /* @__PURE__ */ new Map();
    this._requestHandlerAbortControllers = /* @__PURE__ */ new Map();
    this._notificationHandlers = /* @__PURE__ */ new Map();
    this._responseHandlers = /* @__PURE__ */ new Map();
    this._progressHandlers = /* @__PURE__ */ new Map();
    this._timeoutInfo = /* @__PURE__ */ new Map();
    this.setNotificationHandler(CancelledNotificationSchema, (notification) => {
      const controller = this._requestHandlerAbortControllers.get(notification.params.requestId);
      controller === null || controller === void 0 ? void 0 : controller.abort(notification.params.reason);
    });
    this.setNotificationHandler(ProgressNotificationSchema, (notification) => {
      this._onprogress(notification);
    });
    this.setRequestHandler(
      PingRequestSchema,
      // Automatic pong by default.
      (_request) => ({})
    );
  }
  _setupTimeout(messageId, timeout, maxTotalTimeout, onTimeout, resetTimeoutOnProgress = false) {
    this._timeoutInfo.set(messageId, {
      timeoutId: setTimeout(onTimeout, timeout),
      startTime: Date.now(),
      timeout,
      maxTotalTimeout,
      resetTimeoutOnProgress,
      onTimeout
    });
  }
  _resetTimeout(messageId) {
    const info3 = this._timeoutInfo.get(messageId);
    if (!info3)
      return false;
    const totalElapsed = Date.now() - info3.startTime;
    if (info3.maxTotalTimeout && totalElapsed >= info3.maxTotalTimeout) {
      this._timeoutInfo.delete(messageId);
      throw new McpError(ErrorCode.RequestTimeout, "Maximum total timeout exceeded", { maxTotalTimeout: info3.maxTotalTimeout, totalElapsed });
    }
    clearTimeout(info3.timeoutId);
    info3.timeoutId = setTimeout(info3.onTimeout, info3.timeout);
    return true;
  }
  _cleanupTimeout(messageId) {
    const info3 = this._timeoutInfo.get(messageId);
    if (info3) {
      clearTimeout(info3.timeoutId);
      this._timeoutInfo.delete(messageId);
    }
  }
  /**
   * Attaches to the given transport, starts it, and starts listening for messages.
   *
   * The Protocol object assumes ownership of the Transport, replacing any callbacks that have already been set, and expects that it is the only user of the Transport instance going forward.
   */
  async connect(transport) {
    this._transport = transport;
    this._transport.onclose = () => {
      this._onclose();
    };
    this._transport.onerror = (error3) => {
      this._onerror(error3);
    };
    this._transport.onmessage = (message, extra) => {
      if (isJSONRPCResponse(message) || isJSONRPCError(message)) {
        this._onresponse(message);
      } else if (isJSONRPCRequest(message)) {
        this._onrequest(message, extra);
      } else if (isJSONRPCNotification(message)) {
        this._onnotification(message);
      } else {
        this._onerror(new Error(`Unknown message type: ${JSON.stringify(message)}`));
      }
    };
    await this._transport.start();
  }
  _onclose() {
    var _a;
    const responseHandlers = this._responseHandlers;
    this._responseHandlers = /* @__PURE__ */ new Map();
    this._progressHandlers.clear();
    this._transport = void 0;
    (_a = this.onclose) === null || _a === void 0 ? void 0 : _a.call(this);
    const error3 = new McpError(ErrorCode.ConnectionClosed, "Connection closed");
    for (const handler of responseHandlers.values()) {
      handler(error3);
    }
  }
  _onerror(error3) {
    var _a;
    (_a = this.onerror) === null || _a === void 0 ? void 0 : _a.call(this, error3);
  }
  _onnotification(notification) {
    var _a;
    const handler = (_a = this._notificationHandlers.get(notification.method)) !== null && _a !== void 0 ? _a : this.fallbackNotificationHandler;
    if (handler === void 0) {
      return;
    }
    Promise.resolve().then(() => handler(notification)).catch((error3) => this._onerror(new Error(`Uncaught error in notification handler: ${error3}`)));
  }
  _onrequest(request, extra) {
    var _a, _b, _c;
    const handler = (_a = this._requestHandlers.get(request.method)) !== null && _a !== void 0 ? _a : this.fallbackRequestHandler;
    if (handler === void 0) {
      (_b = this._transport) === null || _b === void 0 ? void 0 : _b.send({
        jsonrpc: "2.0",
        id: request.id,
        error: {
          code: ErrorCode.MethodNotFound,
          message: "Method not found"
        }
      }).catch((error3) => this._onerror(new Error(`Failed to send an error response: ${error3}`)));
      return;
    }
    const abortController = new AbortController();
    this._requestHandlerAbortControllers.set(request.id, abortController);
    const fullExtra = {
      signal: abortController.signal,
      sessionId: (_c = this._transport) === null || _c === void 0 ? void 0 : _c.sessionId,
      sendNotification: /* @__PURE__ */ __name((notification) => this.notification(notification, { relatedRequestId: request.id }), "sendNotification"),
      sendRequest: /* @__PURE__ */ __name((r, resultSchema, options) => this.request(r, resultSchema, { ...options, relatedRequestId: request.id }), "sendRequest"),
      authInfo: extra === null || extra === void 0 ? void 0 : extra.authInfo
    };
    Promise.resolve().then(() => handler(request, fullExtra)).then((result) => {
      var _a2;
      if (abortController.signal.aborted) {
        return;
      }
      return (_a2 = this._transport) === null || _a2 === void 0 ? void 0 : _a2.send({
        result,
        jsonrpc: "2.0",
        id: request.id
      });
    }, (error3) => {
      var _a2, _b2;
      if (abortController.signal.aborted) {
        return;
      }
      return (_a2 = this._transport) === null || _a2 === void 0 ? void 0 : _a2.send({
        jsonrpc: "2.0",
        id: request.id,
        error: {
          code: Number.isSafeInteger(error3["code"]) ? error3["code"] : ErrorCode.InternalError,
          message: (_b2 = error3.message) !== null && _b2 !== void 0 ? _b2 : "Internal error"
        }
      });
    }).catch((error3) => this._onerror(new Error(`Failed to send response: ${error3}`))).finally(() => {
      this._requestHandlerAbortControllers.delete(request.id);
    });
  }
  _onprogress(notification) {
    const { progressToken, ...params } = notification.params;
    const messageId = Number(progressToken);
    const handler = this._progressHandlers.get(messageId);
    if (!handler) {
      this._onerror(new Error(`Received a progress notification for an unknown token: ${JSON.stringify(notification)}`));
      return;
    }
    const responseHandler = this._responseHandlers.get(messageId);
    const timeoutInfo = this._timeoutInfo.get(messageId);
    if (timeoutInfo && responseHandler && timeoutInfo.resetTimeoutOnProgress) {
      try {
        this._resetTimeout(messageId);
      } catch (error3) {
        responseHandler(error3);
        return;
      }
    }
    handler(params);
  }
  _onresponse(response) {
    const messageId = Number(response.id);
    const handler = this._responseHandlers.get(messageId);
    if (handler === void 0) {
      this._onerror(new Error(`Received a response for an unknown message ID: ${JSON.stringify(response)}`));
      return;
    }
    this._responseHandlers.delete(messageId);
    this._progressHandlers.delete(messageId);
    this._cleanupTimeout(messageId);
    if (isJSONRPCResponse(response)) {
      handler(response);
    } else {
      const error3 = new McpError(response.error.code, response.error.message, response.error.data);
      handler(error3);
    }
  }
  get transport() {
    return this._transport;
  }
  /**
   * Closes the connection.
   */
  async close() {
    var _a;
    await ((_a = this._transport) === null || _a === void 0 ? void 0 : _a.close());
  }
  /**
   * Sends a request and wait for a response.
   *
   * Do not use this method to emit notifications! Use notification() instead.
   */
  request(request, resultSchema, options) {
    const { relatedRequestId, resumptionToken, onresumptiontoken } = options !== null && options !== void 0 ? options : {};
    return new Promise((resolve, reject) => {
      var _a, _b, _c, _d, _e;
      if (!this._transport) {
        reject(new Error("Not connected"));
        return;
      }
      if (((_a = this._options) === null || _a === void 0 ? void 0 : _a.enforceStrictCapabilities) === true) {
        this.assertCapabilityForMethod(request.method);
      }
      (_b = options === null || options === void 0 ? void 0 : options.signal) === null || _b === void 0 ? void 0 : _b.throwIfAborted();
      const messageId = this._requestMessageId++;
      const jsonrpcRequest = {
        ...request,
        jsonrpc: "2.0",
        id: messageId
      };
      if (options === null || options === void 0 ? void 0 : options.onprogress) {
        this._progressHandlers.set(messageId, options.onprogress);
        jsonrpcRequest.params = {
          ...request.params,
          _meta: { progressToken: messageId }
        };
      }
      const cancel = /* @__PURE__ */ __name((reason) => {
        var _a2;
        this._responseHandlers.delete(messageId);
        this._progressHandlers.delete(messageId);
        this._cleanupTimeout(messageId);
        (_a2 = this._transport) === null || _a2 === void 0 ? void 0 : _a2.send({
          jsonrpc: "2.0",
          method: "notifications/cancelled",
          params: {
            requestId: messageId,
            reason: String(reason)
          }
        }, { relatedRequestId, resumptionToken, onresumptiontoken }).catch((error3) => this._onerror(new Error(`Failed to send cancellation: ${error3}`)));
        reject(reason);
      }, "cancel");
      this._responseHandlers.set(messageId, (response) => {
        var _a2;
        if ((_a2 = options === null || options === void 0 ? void 0 : options.signal) === null || _a2 === void 0 ? void 0 : _a2.aborted) {
          return;
        }
        if (response instanceof Error) {
          return reject(response);
        }
        try {
          const result = resultSchema.parse(response.result);
          resolve(result);
        } catch (error3) {
          reject(error3);
        }
      });
      (_c = options === null || options === void 0 ? void 0 : options.signal) === null || _c === void 0 ? void 0 : _c.addEventListener("abort", () => {
        var _a2;
        cancel((_a2 = options === null || options === void 0 ? void 0 : options.signal) === null || _a2 === void 0 ? void 0 : _a2.reason);
      });
      const timeout = (_d = options === null || options === void 0 ? void 0 : options.timeout) !== null && _d !== void 0 ? _d : DEFAULT_REQUEST_TIMEOUT_MSEC;
      const timeoutHandler = /* @__PURE__ */ __name(() => cancel(new McpError(ErrorCode.RequestTimeout, "Request timed out", { timeout })), "timeoutHandler");
      this._setupTimeout(messageId, timeout, options === null || options === void 0 ? void 0 : options.maxTotalTimeout, timeoutHandler, (_e = options === null || options === void 0 ? void 0 : options.resetTimeoutOnProgress) !== null && _e !== void 0 ? _e : false);
      this._transport.send(jsonrpcRequest, { relatedRequestId, resumptionToken, onresumptiontoken }).catch((error3) => {
        this._cleanupTimeout(messageId);
        reject(error3);
      });
    });
  }
  /**
   * Emits a notification, which is a one-way message that does not expect a response.
   */
  async notification(notification, options) {
    if (!this._transport) {
      throw new Error("Not connected");
    }
    this.assertNotificationCapability(notification.method);
    const jsonrpcNotification = {
      ...notification,
      jsonrpc: "2.0"
    };
    await this._transport.send(jsonrpcNotification, options);
  }
  /**
   * Registers a handler to invoke when this protocol object receives a request with the given method.
   *
   * Note that this will replace any previous request handler for the same method.
   */
  setRequestHandler(requestSchema, handler) {
    const method = requestSchema.shape.method.value;
    this.assertRequestHandlerCapability(method);
    this._requestHandlers.set(method, (request, extra) => {
      return Promise.resolve(handler(requestSchema.parse(request), extra));
    });
  }
  /**
   * Removes the request handler for the given method.
   */
  removeRequestHandler(method) {
    this._requestHandlers.delete(method);
  }
  /**
   * Asserts that a request handler has not already been set for the given method, in preparation for a new one being automatically installed.
   */
  assertCanSetRequestHandler(method) {
    if (this._requestHandlers.has(method)) {
      throw new Error(`A request handler for ${method} already exists, which would be overridden`);
    }
  }
  /**
   * Registers a handler to invoke when this protocol object receives a notification with the given method.
   *
   * Note that this will replace any previous notification handler for the same method.
   */
  setNotificationHandler(notificationSchema, handler) {
    this._notificationHandlers.set(notificationSchema.shape.method.value, (notification) => Promise.resolve(handler(notificationSchema.parse(notification))));
  }
  /**
   * Removes the notification handler for the given method.
   */
  removeNotificationHandler(method) {
    this._notificationHandlers.delete(method);
  }
};
function mergeCapabilities(base, additional) {
  return Object.entries(additional).reduce((acc, [key, value]) => {
    if (value && typeof value === "object") {
      acc[key] = acc[key] ? { ...acc[key], ...value } : value;
    } else {
      acc[key] = value;
    }
    return acc;
  }, { ...base });
}
__name(mergeCapabilities, "mergeCapabilities");

// node_modules/@modelcontextprotocol/sdk/dist/esm/server/index.js
var Server = class extends Protocol {
  static {
    __name(this, "Server");
  }
  /**
   * Initializes this server with the given name and version information.
   */
  constructor(_serverInfo, options) {
    var _a;
    super(options);
    this._serverInfo = _serverInfo;
    this._capabilities = (_a = options === null || options === void 0 ? void 0 : options.capabilities) !== null && _a !== void 0 ? _a : {};
    this._instructions = options === null || options === void 0 ? void 0 : options.instructions;
    this.setRequestHandler(InitializeRequestSchema, (request) => this._oninitialize(request));
    this.setNotificationHandler(InitializedNotificationSchema, () => {
      var _a2;
      return (_a2 = this.oninitialized) === null || _a2 === void 0 ? void 0 : _a2.call(this);
    });
  }
  /**
   * Registers new capabilities. This can only be called before connecting to a transport.
   *
   * The new capabilities will be merged with any existing capabilities previously given (e.g., at initialization).
   */
  registerCapabilities(capabilities) {
    if (this.transport) {
      throw new Error("Cannot register capabilities after connecting to transport");
    }
    this._capabilities = mergeCapabilities(this._capabilities, capabilities);
  }
  assertCapabilityForMethod(method) {
    var _a, _b;
    switch (method) {
      case "sampling/createMessage":
        if (!((_a = this._clientCapabilities) === null || _a === void 0 ? void 0 : _a.sampling)) {
          throw new Error(`Client does not support sampling (required for ${method})`);
        }
        break;
      case "roots/list":
        if (!((_b = this._clientCapabilities) === null || _b === void 0 ? void 0 : _b.roots)) {
          throw new Error(`Client does not support listing roots (required for ${method})`);
        }
        break;
      case "ping":
        break;
    }
  }
  assertNotificationCapability(method) {
    switch (method) {
      case "notifications/message":
        if (!this._capabilities.logging) {
          throw new Error(`Server does not support logging (required for ${method})`);
        }
        break;
      case "notifications/resources/updated":
      case "notifications/resources/list_changed":
        if (!this._capabilities.resources) {
          throw new Error(`Server does not support notifying about resources (required for ${method})`);
        }
        break;
      case "notifications/tools/list_changed":
        if (!this._capabilities.tools) {
          throw new Error(`Server does not support notifying of tool list changes (required for ${method})`);
        }
        break;
      case "notifications/prompts/list_changed":
        if (!this._capabilities.prompts) {
          throw new Error(`Server does not support notifying of prompt list changes (required for ${method})`);
        }
        break;
      case "notifications/cancelled":
        break;
      case "notifications/progress":
        break;
    }
  }
  assertRequestHandlerCapability(method) {
    switch (method) {
      case "sampling/createMessage":
        if (!this._capabilities.sampling) {
          throw new Error(`Server does not support sampling (required for ${method})`);
        }
        break;
      case "logging/setLevel":
        if (!this._capabilities.logging) {
          throw new Error(`Server does not support logging (required for ${method})`);
        }
        break;
      case "prompts/get":
      case "prompts/list":
        if (!this._capabilities.prompts) {
          throw new Error(`Server does not support prompts (required for ${method})`);
        }
        break;
      case "resources/list":
      case "resources/templates/list":
      case "resources/read":
        if (!this._capabilities.resources) {
          throw new Error(`Server does not support resources (required for ${method})`);
        }
        break;
      case "tools/call":
      case "tools/list":
        if (!this._capabilities.tools) {
          throw new Error(`Server does not support tools (required for ${method})`);
        }
        break;
      case "ping":
      case "initialize":
        break;
    }
  }
  async _oninitialize(request) {
    const requestedVersion = request.params.protocolVersion;
    this._clientCapabilities = request.params.capabilities;
    this._clientVersion = request.params.clientInfo;
    return {
      protocolVersion: SUPPORTED_PROTOCOL_VERSIONS.includes(requestedVersion) ? requestedVersion : LATEST_PROTOCOL_VERSION,
      capabilities: this.getCapabilities(),
      serverInfo: this._serverInfo,
      ...this._instructions && { instructions: this._instructions }
    };
  }
  /**
   * After initialization has completed, this will be populated with the client's reported capabilities.
   */
  getClientCapabilities() {
    return this._clientCapabilities;
  }
  /**
   * After initialization has completed, this will be populated with information about the client's name and version.
   */
  getClientVersion() {
    return this._clientVersion;
  }
  getCapabilities() {
    return this._capabilities;
  }
  async ping() {
    return this.request({ method: "ping" }, EmptyResultSchema);
  }
  async createMessage(params, options) {
    return this.request({ method: "sampling/createMessage", params }, CreateMessageResultSchema, options);
  }
  async listRoots(params, options) {
    return this.request({ method: "roots/list", params }, ListRootsResultSchema, options);
  }
  async sendLoggingMessage(params) {
    return this.notification({ method: "notifications/message", params });
  }
  async sendResourceUpdated(params) {
    return this.notification({
      method: "notifications/resources/updated",
      params
    });
  }
  async sendResourceListChanged() {
    return this.notification({
      method: "notifications/resources/list_changed"
    });
  }
  async sendToolListChanged() {
    return this.notification({ method: "notifications/tools/list_changed" });
  }
  async sendPromptListChanged() {
    return this.notification({ method: "notifications/prompts/list_changed" });
  }
};

// node_modules/@modelcontextprotocol/sdk/dist/esm/shared/stdio.js
var ReadBuffer = class {
  static {
    __name(this, "ReadBuffer");
  }
  append(chunk) {
    this._buffer = this._buffer ? Buffer.concat([this._buffer, chunk]) : chunk;
  }
  readMessage() {
    if (!this._buffer) {
      return null;
    }
    const index = this._buffer.indexOf("\n");
    if (index === -1) {
      return null;
    }
    const line = this._buffer.toString("utf8", 0, index).replace(/\r$/, "");
    this._buffer = this._buffer.subarray(index + 1);
    return deserializeMessage(line);
  }
  clear() {
    this._buffer = void 0;
  }
};
function deserializeMessage(line) {
  return JSONRPCMessageSchema.parse(JSON.parse(line));
}
__name(deserializeMessage, "deserializeMessage");
function serializeMessage(message) {
  return JSON.stringify(message) + "\n";
}
__name(serializeMessage, "serializeMessage");

// node_modules/@modelcontextprotocol/sdk/dist/esm/server/stdio.js
var StdioServerTransport = class {
  static {
    __name(this, "StdioServerTransport");
  }
  constructor(_stdin = process_default.stdin, _stdout2 = process_default.stdout) {
    this._stdin = _stdin;
    this._stdout = _stdout2;
    this._readBuffer = new ReadBuffer();
    this._started = false;
    this._ondata = (chunk) => {
      this._readBuffer.append(chunk);
      this.processReadBuffer();
    };
    this._onerror = (error3) => {
      var _a;
      (_a = this.onerror) === null || _a === void 0 ? void 0 : _a.call(this, error3);
    };
  }
  /**
   * Starts listening for messages on stdin.
   */
  async start() {
    if (this._started) {
      throw new Error("StdioServerTransport already started! If using Server class, note that connect() calls start() automatically.");
    }
    this._started = true;
    this._stdin.on("data", this._ondata);
    this._stdin.on("error", this._onerror);
  }
  processReadBuffer() {
    var _a, _b;
    while (true) {
      try {
        const message = this._readBuffer.readMessage();
        if (message === null) {
          break;
        }
        (_a = this.onmessage) === null || _a === void 0 ? void 0 : _a.call(this, message);
      } catch (error3) {
        (_b = this.onerror) === null || _b === void 0 ? void 0 : _b.call(this, error3);
      }
    }
  }
  async close() {
    var _a;
    this._stdin.off("data", this._ondata);
    this._stdin.off("error", this._onerror);
    const remainingDataListeners = this._stdin.listenerCount("data");
    if (remainingDataListeners === 0) {
      this._stdin.pause();
    }
    this._readBuffer.clear();
    (_a = this.onclose) === null || _a === void 0 ? void 0 : _a.call(this);
  }
  send(message) {
    return new Promise((resolve) => {
      const json = serializeMessage(message);
      if (this._stdout.write(json)) {
        resolve();
      } else {
        this._stdout.once("drain", resolve);
      }
    });
  }
};

// node_modules/zod-to-json-schema/dist/esm/Options.js
var ignoreOverride = Symbol("Let zodToJsonSchema decide on which parser to use");
var defaultOptions = {
  name: void 0,
  $refStrategy: "root",
  basePath: ["#"],
  effectStrategy: "input",
  pipeStrategy: "all",
  dateStrategy: "format:date-time",
  mapStrategy: "entries",
  removeAdditionalStrategy: "passthrough",
  allowedAdditionalProperties: true,
  rejectedAdditionalProperties: false,
  definitionPath: "definitions",
  target: "jsonSchema7",
  strictUnions: false,
  definitions: {},
  errorMessages: false,
  markdownDescription: false,
  patternStrategy: "escape",
  applyRegexFlags: false,
  emailStrategy: "format:email",
  base64Strategy: "contentEncoding:base64",
  nameStrategy: "ref"
};
var getDefaultOptions = /* @__PURE__ */ __name((options) => typeof options === "string" ? {
  ...defaultOptions,
  name: options
} : {
  ...defaultOptions,
  ...options
}, "getDefaultOptions");

// node_modules/zod-to-json-schema/dist/esm/Refs.js
var getRefs = /* @__PURE__ */ __name((options) => {
  const _options = getDefaultOptions(options);
  const currentPath = _options.name !== void 0 ? [..._options.basePath, _options.definitionPath, _options.name] : _options.basePath;
  return {
    ..._options,
    currentPath,
    propertyPath: void 0,
    seen: new Map(Object.entries(_options.definitions).map(([name, def]) => [
      def._def,
      {
        def: def._def,
        path: [..._options.basePath, _options.definitionPath, name],
        // Resolution of references will be forced even though seen, so it's ok that the schema is undefined here for now.
        jsonSchema: void 0
      }
    ]))
  };
}, "getRefs");

// node_modules/zod-to-json-schema/dist/esm/errorMessages.js
function addErrorMessage(res, key, errorMessage, refs) {
  if (!refs?.errorMessages)
    return;
  if (errorMessage) {
    res.errorMessage = {
      ...res.errorMessage,
      [key]: errorMessage
    };
  }
}
__name(addErrorMessage, "addErrorMessage");
function setResponseValueAndErrors(res, key, value, errorMessage, refs) {
  res[key] = value;
  addErrorMessage(res, key, errorMessage, refs);
}
__name(setResponseValueAndErrors, "setResponseValueAndErrors");

// node_modules/zod-to-json-schema/dist/esm/parsers/any.js
function parseAnyDef() {
  return {};
}
__name(parseAnyDef, "parseAnyDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/array.js
function parseArrayDef(def, refs) {
  const res = {
    type: "array"
  };
  if (def.type?._def && def.type?._def?.typeName !== ZodFirstPartyTypeKind.ZodAny) {
    res.items = parseDef(def.type._def, {
      ...refs,
      currentPath: [...refs.currentPath, "items"]
    });
  }
  if (def.minLength) {
    setResponseValueAndErrors(res, "minItems", def.minLength.value, def.minLength.message, refs);
  }
  if (def.maxLength) {
    setResponseValueAndErrors(res, "maxItems", def.maxLength.value, def.maxLength.message, refs);
  }
  if (def.exactLength) {
    setResponseValueAndErrors(res, "minItems", def.exactLength.value, def.exactLength.message, refs);
    setResponseValueAndErrors(res, "maxItems", def.exactLength.value, def.exactLength.message, refs);
  }
  return res;
}
__name(parseArrayDef, "parseArrayDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/bigint.js
function parseBigintDef(def, refs) {
  const res = {
    type: "integer",
    format: "int64"
  };
  if (!def.checks)
    return res;
  for (const check of def.checks) {
    switch (check.kind) {
      case "min":
        if (refs.target === "jsonSchema7") {
          if (check.inclusive) {
            setResponseValueAndErrors(res, "minimum", check.value, check.message, refs);
          } else {
            setResponseValueAndErrors(res, "exclusiveMinimum", check.value, check.message, refs);
          }
        } else {
          if (!check.inclusive) {
            res.exclusiveMinimum = true;
          }
          setResponseValueAndErrors(res, "minimum", check.value, check.message, refs);
        }
        break;
      case "max":
        if (refs.target === "jsonSchema7") {
          if (check.inclusive) {
            setResponseValueAndErrors(res, "maximum", check.value, check.message, refs);
          } else {
            setResponseValueAndErrors(res, "exclusiveMaximum", check.value, check.message, refs);
          }
        } else {
          if (!check.inclusive) {
            res.exclusiveMaximum = true;
          }
          setResponseValueAndErrors(res, "maximum", check.value, check.message, refs);
        }
        break;
      case "multipleOf":
        setResponseValueAndErrors(res, "multipleOf", check.value, check.message, refs);
        break;
    }
  }
  return res;
}
__name(parseBigintDef, "parseBigintDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/boolean.js
function parseBooleanDef() {
  return {
    type: "boolean"
  };
}
__name(parseBooleanDef, "parseBooleanDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/branded.js
function parseBrandedDef(_def, refs) {
  return parseDef(_def.type._def, refs);
}
__name(parseBrandedDef, "parseBrandedDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/catch.js
var parseCatchDef = /* @__PURE__ */ __name((def, refs) => {
  return parseDef(def.innerType._def, refs);
}, "parseCatchDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/date.js
function parseDateDef(def, refs, overrideDateStrategy) {
  const strategy = overrideDateStrategy ?? refs.dateStrategy;
  if (Array.isArray(strategy)) {
    return {
      anyOf: strategy.map((item, i) => parseDateDef(def, refs, item))
    };
  }
  switch (strategy) {
    case "string":
    case "format:date-time":
      return {
        type: "string",
        format: "date-time"
      };
    case "format:date":
      return {
        type: "string",
        format: "date"
      };
    case "integer":
      return integerDateParser(def, refs);
  }
}
__name(parseDateDef, "parseDateDef");
var integerDateParser = /* @__PURE__ */ __name((def, refs) => {
  const res = {
    type: "integer",
    format: "unix-time"
  };
  if (refs.target === "openApi3") {
    return res;
  }
  for (const check of def.checks) {
    switch (check.kind) {
      case "min":
        setResponseValueAndErrors(
          res,
          "minimum",
          check.value,
          // This is in milliseconds
          check.message,
          refs
        );
        break;
      case "max":
        setResponseValueAndErrors(
          res,
          "maximum",
          check.value,
          // This is in milliseconds
          check.message,
          refs
        );
        break;
    }
  }
  return res;
}, "integerDateParser");

// node_modules/zod-to-json-schema/dist/esm/parsers/default.js
function parseDefaultDef(_def, refs) {
  return {
    ...parseDef(_def.innerType._def, refs),
    default: _def.defaultValue()
  };
}
__name(parseDefaultDef, "parseDefaultDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/effects.js
function parseEffectsDef(_def, refs) {
  return refs.effectStrategy === "input" ? parseDef(_def.schema._def, refs) : {};
}
__name(parseEffectsDef, "parseEffectsDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/enum.js
function parseEnumDef(def) {
  return {
    type: "string",
    enum: Array.from(def.values)
  };
}
__name(parseEnumDef, "parseEnumDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/intersection.js
var isJsonSchema7AllOfType = /* @__PURE__ */ __name((type) => {
  if ("type" in type && type.type === "string")
    return false;
  return "allOf" in type;
}, "isJsonSchema7AllOfType");
function parseIntersectionDef(def, refs) {
  const allOf = [
    parseDef(def.left._def, {
      ...refs,
      currentPath: [...refs.currentPath, "allOf", "0"]
    }),
    parseDef(def.right._def, {
      ...refs,
      currentPath: [...refs.currentPath, "allOf", "1"]
    })
  ].filter((x) => !!x);
  let unevaluatedProperties = refs.target === "jsonSchema2019-09" ? { unevaluatedProperties: false } : void 0;
  const mergedAllOf = [];
  allOf.forEach((schema) => {
    if (isJsonSchema7AllOfType(schema)) {
      mergedAllOf.push(...schema.allOf);
      if (schema.unevaluatedProperties === void 0) {
        unevaluatedProperties = void 0;
      }
    } else {
      let nestedSchema = schema;
      if ("additionalProperties" in schema && schema.additionalProperties === false) {
        const { additionalProperties, ...rest } = schema;
        nestedSchema = rest;
      } else {
        unevaluatedProperties = void 0;
      }
      mergedAllOf.push(nestedSchema);
    }
  });
  return mergedAllOf.length ? {
    allOf: mergedAllOf,
    ...unevaluatedProperties
  } : void 0;
}
__name(parseIntersectionDef, "parseIntersectionDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/literal.js
function parseLiteralDef(def, refs) {
  const parsedType = typeof def.value;
  if (parsedType !== "bigint" && parsedType !== "number" && parsedType !== "boolean" && parsedType !== "string") {
    return {
      type: Array.isArray(def.value) ? "array" : "object"
    };
  }
  if (refs.target === "openApi3") {
    return {
      type: parsedType === "bigint" ? "integer" : parsedType,
      enum: [def.value]
    };
  }
  return {
    type: parsedType === "bigint" ? "integer" : parsedType,
    const: def.value
  };
}
__name(parseLiteralDef, "parseLiteralDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/string.js
var emojiRegex2 = void 0;
var zodPatterns = {
  /**
   * `c` was changed to `[cC]` to replicate /i flag
   */
  cuid: /^[cC][^\s-]{8,}$/,
  cuid2: /^[0-9a-z]+$/,
  ulid: /^[0-9A-HJKMNP-TV-Z]{26}$/,
  /**
   * `a-z` was added to replicate /i flag
   */
  email: /^(?!\.)(?!.*\.\.)([a-zA-Z0-9_'+\-\.]*)[a-zA-Z0-9_+-]@([a-zA-Z0-9][a-zA-Z0-9\-]*\.)+[a-zA-Z]{2,}$/,
  /**
   * Constructed a valid Unicode RegExp
   *
   * Lazily instantiate since this type of regex isn't supported
   * in all envs (e.g. React Native).
   *
   * See:
   * https://github.com/colinhacks/zod/issues/2433
   * Fix in Zod:
   * https://github.com/colinhacks/zod/commit/9340fd51e48576a75adc919bff65dbc4a5d4c99b
   */
  emoji: /* @__PURE__ */ __name(() => {
    if (emojiRegex2 === void 0) {
      emojiRegex2 = RegExp("^(\\p{Extended_Pictographic}|\\p{Emoji_Component})+$", "u");
    }
    return emojiRegex2;
  }, "emoji"),
  /**
   * Unused
   */
  uuid: /^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/,
  /**
   * Unused
   */
  ipv4: /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/,
  ipv4Cidr: /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/(3[0-2]|[12]?[0-9])$/,
  /**
   * Unused
   */
  ipv6: /^(([a-f0-9]{1,4}:){7}|::([a-f0-9]{1,4}:){0,6}|([a-f0-9]{1,4}:){1}:([a-f0-9]{1,4}:){0,5}|([a-f0-9]{1,4}:){2}:([a-f0-9]{1,4}:){0,4}|([a-f0-9]{1,4}:){3}:([a-f0-9]{1,4}:){0,3}|([a-f0-9]{1,4}:){4}:([a-f0-9]{1,4}:){0,2}|([a-f0-9]{1,4}:){5}:([a-f0-9]{1,4}:){0,1})([a-f0-9]{1,4}|(((25[0-5])|(2[0-4][0-9])|(1[0-9]{2})|([0-9]{1,2}))\.){3}((25[0-5])|(2[0-4][0-9])|(1[0-9]{2})|([0-9]{1,2})))$/,
  ipv6Cidr: /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/,
  base64: /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/,
  base64url: /^([0-9a-zA-Z-_]{4})*(([0-9a-zA-Z-_]{2}(==)?)|([0-9a-zA-Z-_]{3}(=)?))?$/,
  nanoid: /^[a-zA-Z0-9_-]{21}$/,
  jwt: /^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$/
};
function parseStringDef(def, refs) {
  const res = {
    type: "string"
  };
  if (def.checks) {
    for (const check of def.checks) {
      switch (check.kind) {
        case "min":
          setResponseValueAndErrors(res, "minLength", typeof res.minLength === "number" ? Math.max(res.minLength, check.value) : check.value, check.message, refs);
          break;
        case "max":
          setResponseValueAndErrors(res, "maxLength", typeof res.maxLength === "number" ? Math.min(res.maxLength, check.value) : check.value, check.message, refs);
          break;
        case "email":
          switch (refs.emailStrategy) {
            case "format:email":
              addFormat(res, "email", check.message, refs);
              break;
            case "format:idn-email":
              addFormat(res, "idn-email", check.message, refs);
              break;
            case "pattern:zod":
              addPattern(res, zodPatterns.email, check.message, refs);
              break;
          }
          break;
        case "url":
          addFormat(res, "uri", check.message, refs);
          break;
        case "uuid":
          addFormat(res, "uuid", check.message, refs);
          break;
        case "regex":
          addPattern(res, check.regex, check.message, refs);
          break;
        case "cuid":
          addPattern(res, zodPatterns.cuid, check.message, refs);
          break;
        case "cuid2":
          addPattern(res, zodPatterns.cuid2, check.message, refs);
          break;
        case "startsWith":
          addPattern(res, RegExp(`^${escapeLiteralCheckValue(check.value, refs)}`), check.message, refs);
          break;
        case "endsWith":
          addPattern(res, RegExp(`${escapeLiteralCheckValue(check.value, refs)}$`), check.message, refs);
          break;
        case "datetime":
          addFormat(res, "date-time", check.message, refs);
          break;
        case "date":
          addFormat(res, "date", check.message, refs);
          break;
        case "time":
          addFormat(res, "time", check.message, refs);
          break;
        case "duration":
          addFormat(res, "duration", check.message, refs);
          break;
        case "length":
          setResponseValueAndErrors(res, "minLength", typeof res.minLength === "number" ? Math.max(res.minLength, check.value) : check.value, check.message, refs);
          setResponseValueAndErrors(res, "maxLength", typeof res.maxLength === "number" ? Math.min(res.maxLength, check.value) : check.value, check.message, refs);
          break;
        case "includes": {
          addPattern(res, RegExp(escapeLiteralCheckValue(check.value, refs)), check.message, refs);
          break;
        }
        case "ip": {
          if (check.version !== "v6") {
            addFormat(res, "ipv4", check.message, refs);
          }
          if (check.version !== "v4") {
            addFormat(res, "ipv6", check.message, refs);
          }
          break;
        }
        case "base64url":
          addPattern(res, zodPatterns.base64url, check.message, refs);
          break;
        case "jwt":
          addPattern(res, zodPatterns.jwt, check.message, refs);
          break;
        case "cidr": {
          if (check.version !== "v6") {
            addPattern(res, zodPatterns.ipv4Cidr, check.message, refs);
          }
          if (check.version !== "v4") {
            addPattern(res, zodPatterns.ipv6Cidr, check.message, refs);
          }
          break;
        }
        case "emoji":
          addPattern(res, zodPatterns.emoji(), check.message, refs);
          break;
        case "ulid": {
          addPattern(res, zodPatterns.ulid, check.message, refs);
          break;
        }
        case "base64": {
          switch (refs.base64Strategy) {
            case "format:binary": {
              addFormat(res, "binary", check.message, refs);
              break;
            }
            case "contentEncoding:base64": {
              setResponseValueAndErrors(res, "contentEncoding", "base64", check.message, refs);
              break;
            }
            case "pattern:zod": {
              addPattern(res, zodPatterns.base64, check.message, refs);
              break;
            }
          }
          break;
        }
        case "nanoid": {
          addPattern(res, zodPatterns.nanoid, check.message, refs);
        }
        case "toLowerCase":
        case "toUpperCase":
        case "trim":
          break;
        default:
          /* @__PURE__ */ ((_) => {
          })(check);
      }
    }
  }
  return res;
}
__name(parseStringDef, "parseStringDef");
function escapeLiteralCheckValue(literal, refs) {
  return refs.patternStrategy === "escape" ? escapeNonAlphaNumeric(literal) : literal;
}
__name(escapeLiteralCheckValue, "escapeLiteralCheckValue");
var ALPHA_NUMERIC = new Set("ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz0123456789");
function escapeNonAlphaNumeric(source) {
  let result = "";
  for (let i = 0; i < source.length; i++) {
    if (!ALPHA_NUMERIC.has(source[i])) {
      result += "\\";
    }
    result += source[i];
  }
  return result;
}
__name(escapeNonAlphaNumeric, "escapeNonAlphaNumeric");
function addFormat(schema, value, message, refs) {
  if (schema.format || schema.anyOf?.some((x) => x.format)) {
    if (!schema.anyOf) {
      schema.anyOf = [];
    }
    if (schema.format) {
      schema.anyOf.push({
        format: schema.format,
        ...schema.errorMessage && refs.errorMessages && {
          errorMessage: { format: schema.errorMessage.format }
        }
      });
      delete schema.format;
      if (schema.errorMessage) {
        delete schema.errorMessage.format;
        if (Object.keys(schema.errorMessage).length === 0) {
          delete schema.errorMessage;
        }
      }
    }
    schema.anyOf.push({
      format: value,
      ...message && refs.errorMessages && { errorMessage: { format: message } }
    });
  } else {
    setResponseValueAndErrors(schema, "format", value, message, refs);
  }
}
__name(addFormat, "addFormat");
function addPattern(schema, regex, message, refs) {
  if (schema.pattern || schema.allOf?.some((x) => x.pattern)) {
    if (!schema.allOf) {
      schema.allOf = [];
    }
    if (schema.pattern) {
      schema.allOf.push({
        pattern: schema.pattern,
        ...schema.errorMessage && refs.errorMessages && {
          errorMessage: { pattern: schema.errorMessage.pattern }
        }
      });
      delete schema.pattern;
      if (schema.errorMessage) {
        delete schema.errorMessage.pattern;
        if (Object.keys(schema.errorMessage).length === 0) {
          delete schema.errorMessage;
        }
      }
    }
    schema.allOf.push({
      pattern: stringifyRegExpWithFlags(regex, refs),
      ...message && refs.errorMessages && { errorMessage: { pattern: message } }
    });
  } else {
    setResponseValueAndErrors(schema, "pattern", stringifyRegExpWithFlags(regex, refs), message, refs);
  }
}
__name(addPattern, "addPattern");
function stringifyRegExpWithFlags(regex, refs) {
  if (!refs.applyRegexFlags || !regex.flags) {
    return regex.source;
  }
  const flags = {
    i: regex.flags.includes("i"),
    m: regex.flags.includes("m"),
    s: regex.flags.includes("s")
    // `.` matches newlines
  };
  const source = flags.i ? regex.source.toLowerCase() : regex.source;
  let pattern = "";
  let isEscaped = false;
  let inCharGroup = false;
  let inCharRange = false;
  for (let i = 0; i < source.length; i++) {
    if (isEscaped) {
      pattern += source[i];
      isEscaped = false;
      continue;
    }
    if (flags.i) {
      if (inCharGroup) {
        if (source[i].match(/[a-z]/)) {
          if (inCharRange) {
            pattern += source[i];
            pattern += `${source[i - 2]}-${source[i]}`.toUpperCase();
            inCharRange = false;
          } else if (source[i + 1] === "-" && source[i + 2]?.match(/[a-z]/)) {
            pattern += source[i];
            inCharRange = true;
          } else {
            pattern += `${source[i]}${source[i].toUpperCase()}`;
          }
          continue;
        }
      } else if (source[i].match(/[a-z]/)) {
        pattern += `[${source[i]}${source[i].toUpperCase()}]`;
        continue;
      }
    }
    if (flags.m) {
      if (source[i] === "^") {
        pattern += `(^|(?<=[\r
]))`;
        continue;
      } else if (source[i] === "$") {
        pattern += `($|(?=[\r
]))`;
        continue;
      }
    }
    if (flags.s && source[i] === ".") {
      pattern += inCharGroup ? `${source[i]}\r
` : `[${source[i]}\r
]`;
      continue;
    }
    pattern += source[i];
    if (source[i] === "\\") {
      isEscaped = true;
    } else if (inCharGroup && source[i] === "]") {
      inCharGroup = false;
    } else if (!inCharGroup && source[i] === "[") {
      inCharGroup = true;
    }
  }
  try {
    new RegExp(pattern);
  } catch {
    console.warn(`Could not convert regex pattern at ${refs.currentPath.join("/")} to a flag-independent form! Falling back to the flag-ignorant source`);
    return regex.source;
  }
  return pattern;
}
__name(stringifyRegExpWithFlags, "stringifyRegExpWithFlags");

// node_modules/zod-to-json-schema/dist/esm/parsers/record.js
function parseRecordDef(def, refs) {
  if (refs.target === "openAi") {
    console.warn("Warning: OpenAI may not support records in schemas! Try an array of key-value pairs instead.");
  }
  if (refs.target === "openApi3" && def.keyType?._def.typeName === ZodFirstPartyTypeKind.ZodEnum) {
    return {
      type: "object",
      required: def.keyType._def.values,
      properties: def.keyType._def.values.reduce((acc, key) => ({
        ...acc,
        [key]: parseDef(def.valueType._def, {
          ...refs,
          currentPath: [...refs.currentPath, "properties", key]
        }) ?? {}
      }), {}),
      additionalProperties: refs.rejectedAdditionalProperties
    };
  }
  const schema = {
    type: "object",
    additionalProperties: parseDef(def.valueType._def, {
      ...refs,
      currentPath: [...refs.currentPath, "additionalProperties"]
    }) ?? refs.allowedAdditionalProperties
  };
  if (refs.target === "openApi3") {
    return schema;
  }
  if (def.keyType?._def.typeName === ZodFirstPartyTypeKind.ZodString && def.keyType._def.checks?.length) {
    const { type, ...keyType } = parseStringDef(def.keyType._def, refs);
    return {
      ...schema,
      propertyNames: keyType
    };
  } else if (def.keyType?._def.typeName === ZodFirstPartyTypeKind.ZodEnum) {
    return {
      ...schema,
      propertyNames: {
        enum: def.keyType._def.values
      }
    };
  } else if (def.keyType?._def.typeName === ZodFirstPartyTypeKind.ZodBranded && def.keyType._def.type._def.typeName === ZodFirstPartyTypeKind.ZodString && def.keyType._def.type._def.checks?.length) {
    const { type, ...keyType } = parseBrandedDef(def.keyType._def, refs);
    return {
      ...schema,
      propertyNames: keyType
    };
  }
  return schema;
}
__name(parseRecordDef, "parseRecordDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/map.js
function parseMapDef(def, refs) {
  if (refs.mapStrategy === "record") {
    return parseRecordDef(def, refs);
  }
  const keys = parseDef(def.keyType._def, {
    ...refs,
    currentPath: [...refs.currentPath, "items", "items", "0"]
  }) || {};
  const values = parseDef(def.valueType._def, {
    ...refs,
    currentPath: [...refs.currentPath, "items", "items", "1"]
  }) || {};
  return {
    type: "array",
    maxItems: 125,
    items: {
      type: "array",
      items: [keys, values],
      minItems: 2,
      maxItems: 2
    }
  };
}
__name(parseMapDef, "parseMapDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/nativeEnum.js
function parseNativeEnumDef(def) {
  const object = def.values;
  const actualKeys = Object.keys(def.values).filter((key) => {
    return typeof object[object[key]] !== "number";
  });
  const actualValues = actualKeys.map((key) => object[key]);
  const parsedTypes = Array.from(new Set(actualValues.map((values) => typeof values)));
  return {
    type: parsedTypes.length === 1 ? parsedTypes[0] === "string" ? "string" : "number" : ["string", "number"],
    enum: actualValues
  };
}
__name(parseNativeEnumDef, "parseNativeEnumDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/never.js
function parseNeverDef() {
  return {
    not: {}
  };
}
__name(parseNeverDef, "parseNeverDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/null.js
function parseNullDef(refs) {
  return refs.target === "openApi3" ? {
    enum: ["null"],
    nullable: true
  } : {
    type: "null"
  };
}
__name(parseNullDef, "parseNullDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/union.js
var primitiveMappings = {
  ZodString: "string",
  ZodNumber: "number",
  ZodBigInt: "integer",
  ZodBoolean: "boolean",
  ZodNull: "null"
};
function parseUnionDef(def, refs) {
  if (refs.target === "openApi3")
    return asAnyOf(def, refs);
  const options = def.options instanceof Map ? Array.from(def.options.values()) : def.options;
  if (options.every((x) => x._def.typeName in primitiveMappings && (!x._def.checks || !x._def.checks.length))) {
    const types = options.reduce((types2, x) => {
      const type = primitiveMappings[x._def.typeName];
      return type && !types2.includes(type) ? [...types2, type] : types2;
    }, []);
    return {
      type: types.length > 1 ? types : types[0]
    };
  } else if (options.every((x) => x._def.typeName === "ZodLiteral" && !x.description)) {
    const types = options.reduce((acc, x) => {
      const type = typeof x._def.value;
      switch (type) {
        case "string":
        case "number":
        case "boolean":
          return [...acc, type];
        case "bigint":
          return [...acc, "integer"];
        case "object":
          if (x._def.value === null)
            return [...acc, "null"];
        case "symbol":
        case "undefined":
        case "function":
        default:
          return acc;
      }
    }, []);
    if (types.length === options.length) {
      const uniqueTypes = types.filter((x, i, a) => a.indexOf(x) === i);
      return {
        type: uniqueTypes.length > 1 ? uniqueTypes : uniqueTypes[0],
        enum: options.reduce((acc, x) => {
          return acc.includes(x._def.value) ? acc : [...acc, x._def.value];
        }, [])
      };
    }
  } else if (options.every((x) => x._def.typeName === "ZodEnum")) {
    return {
      type: "string",
      enum: options.reduce((acc, x) => [
        ...acc,
        ...x._def.values.filter((x2) => !acc.includes(x2))
      ], [])
    };
  }
  return asAnyOf(def, refs);
}
__name(parseUnionDef, "parseUnionDef");
var asAnyOf = /* @__PURE__ */ __name((def, refs) => {
  const anyOf = (def.options instanceof Map ? Array.from(def.options.values()) : def.options).map((x, i) => parseDef(x._def, {
    ...refs,
    currentPath: [...refs.currentPath, "anyOf", `${i}`]
  })).filter((x) => !!x && (!refs.strictUnions || typeof x === "object" && Object.keys(x).length > 0));
  return anyOf.length ? { anyOf } : void 0;
}, "asAnyOf");

// node_modules/zod-to-json-schema/dist/esm/parsers/nullable.js
function parseNullableDef(def, refs) {
  if (["ZodString", "ZodNumber", "ZodBigInt", "ZodBoolean", "ZodNull"].includes(def.innerType._def.typeName) && (!def.innerType._def.checks || !def.innerType._def.checks.length)) {
    if (refs.target === "openApi3") {
      return {
        type: primitiveMappings[def.innerType._def.typeName],
        nullable: true
      };
    }
    return {
      type: [
        primitiveMappings[def.innerType._def.typeName],
        "null"
      ]
    };
  }
  if (refs.target === "openApi3") {
    const base2 = parseDef(def.innerType._def, {
      ...refs,
      currentPath: [...refs.currentPath]
    });
    if (base2 && "$ref" in base2)
      return { allOf: [base2], nullable: true };
    return base2 && { ...base2, nullable: true };
  }
  const base = parseDef(def.innerType._def, {
    ...refs,
    currentPath: [...refs.currentPath, "anyOf", "0"]
  });
  return base && { anyOf: [base, { type: "null" }] };
}
__name(parseNullableDef, "parseNullableDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/number.js
function parseNumberDef(def, refs) {
  const res = {
    type: "number"
  };
  if (!def.checks)
    return res;
  for (const check of def.checks) {
    switch (check.kind) {
      case "int":
        res.type = "integer";
        addErrorMessage(res, "type", check.message, refs);
        break;
      case "min":
        if (refs.target === "jsonSchema7") {
          if (check.inclusive) {
            setResponseValueAndErrors(res, "minimum", check.value, check.message, refs);
          } else {
            setResponseValueAndErrors(res, "exclusiveMinimum", check.value, check.message, refs);
          }
        } else {
          if (!check.inclusive) {
            res.exclusiveMinimum = true;
          }
          setResponseValueAndErrors(res, "minimum", check.value, check.message, refs);
        }
        break;
      case "max":
        if (refs.target === "jsonSchema7") {
          if (check.inclusive) {
            setResponseValueAndErrors(res, "maximum", check.value, check.message, refs);
          } else {
            setResponseValueAndErrors(res, "exclusiveMaximum", check.value, check.message, refs);
          }
        } else {
          if (!check.inclusive) {
            res.exclusiveMaximum = true;
          }
          setResponseValueAndErrors(res, "maximum", check.value, check.message, refs);
        }
        break;
      case "multipleOf":
        setResponseValueAndErrors(res, "multipleOf", check.value, check.message, refs);
        break;
    }
  }
  return res;
}
__name(parseNumberDef, "parseNumberDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/object.js
function parseObjectDef(def, refs) {
  const forceOptionalIntoNullable = refs.target === "openAi";
  const result = {
    type: "object",
    properties: {}
  };
  const required = [];
  const shape = def.shape();
  for (const propName in shape) {
    let propDef = shape[propName];
    if (propDef === void 0 || propDef._def === void 0) {
      continue;
    }
    let propOptional = safeIsOptional(propDef);
    if (propOptional && forceOptionalIntoNullable) {
      if (propDef instanceof ZodOptional) {
        propDef = propDef._def.innerType;
      }
      if (!propDef.isNullable()) {
        propDef = propDef.nullable();
      }
      propOptional = false;
    }
    const parsedDef = parseDef(propDef._def, {
      ...refs,
      currentPath: [...refs.currentPath, "properties", propName],
      propertyPath: [...refs.currentPath, "properties", propName]
    });
    if (parsedDef === void 0) {
      continue;
    }
    result.properties[propName] = parsedDef;
    if (!propOptional) {
      required.push(propName);
    }
  }
  if (required.length) {
    result.required = required;
  }
  const additionalProperties = decideAdditionalProperties(def, refs);
  if (additionalProperties !== void 0) {
    result.additionalProperties = additionalProperties;
  }
  return result;
}
__name(parseObjectDef, "parseObjectDef");
function decideAdditionalProperties(def, refs) {
  if (def.catchall._def.typeName !== "ZodNever") {
    return parseDef(def.catchall._def, {
      ...refs,
      currentPath: [...refs.currentPath, "additionalProperties"]
    });
  }
  switch (def.unknownKeys) {
    case "passthrough":
      return refs.allowedAdditionalProperties;
    case "strict":
      return refs.rejectedAdditionalProperties;
    case "strip":
      return refs.removeAdditionalStrategy === "strict" ? refs.allowedAdditionalProperties : refs.rejectedAdditionalProperties;
  }
}
__name(decideAdditionalProperties, "decideAdditionalProperties");
function safeIsOptional(schema) {
  try {
    return schema.isOptional();
  } catch {
    return true;
  }
}
__name(safeIsOptional, "safeIsOptional");

// node_modules/zod-to-json-schema/dist/esm/parsers/optional.js
var parseOptionalDef = /* @__PURE__ */ __name((def, refs) => {
  if (refs.currentPath.toString() === refs.propertyPath?.toString()) {
    return parseDef(def.innerType._def, refs);
  }
  const innerSchema = parseDef(def.innerType._def, {
    ...refs,
    currentPath: [...refs.currentPath, "anyOf", "1"]
  });
  return innerSchema ? {
    anyOf: [
      {
        not: {}
      },
      innerSchema
    ]
  } : {};
}, "parseOptionalDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/pipeline.js
var parsePipelineDef = /* @__PURE__ */ __name((def, refs) => {
  if (refs.pipeStrategy === "input") {
    return parseDef(def.in._def, refs);
  } else if (refs.pipeStrategy === "output") {
    return parseDef(def.out._def, refs);
  }
  const a = parseDef(def.in._def, {
    ...refs,
    currentPath: [...refs.currentPath, "allOf", "0"]
  });
  const b = parseDef(def.out._def, {
    ...refs,
    currentPath: [...refs.currentPath, "allOf", a ? "1" : "0"]
  });
  return {
    allOf: [a, b].filter((x) => x !== void 0)
  };
}, "parsePipelineDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/promise.js
function parsePromiseDef(def, refs) {
  return parseDef(def.type._def, refs);
}
__name(parsePromiseDef, "parsePromiseDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/set.js
function parseSetDef(def, refs) {
  const items = parseDef(def.valueType._def, {
    ...refs,
    currentPath: [...refs.currentPath, "items"]
  });
  const schema = {
    type: "array",
    uniqueItems: true,
    items
  };
  if (def.minSize) {
    setResponseValueAndErrors(schema, "minItems", def.minSize.value, def.minSize.message, refs);
  }
  if (def.maxSize) {
    setResponseValueAndErrors(schema, "maxItems", def.maxSize.value, def.maxSize.message, refs);
  }
  return schema;
}
__name(parseSetDef, "parseSetDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/tuple.js
function parseTupleDef(def, refs) {
  if (def.rest) {
    return {
      type: "array",
      minItems: def.items.length,
      items: def.items.map((x, i) => parseDef(x._def, {
        ...refs,
        currentPath: [...refs.currentPath, "items", `${i}`]
      })).reduce((acc, x) => x === void 0 ? acc : [...acc, x], []),
      additionalItems: parseDef(def.rest._def, {
        ...refs,
        currentPath: [...refs.currentPath, "additionalItems"]
      })
    };
  } else {
    return {
      type: "array",
      minItems: def.items.length,
      maxItems: def.items.length,
      items: def.items.map((x, i) => parseDef(x._def, {
        ...refs,
        currentPath: [...refs.currentPath, "items", `${i}`]
      })).reduce((acc, x) => x === void 0 ? acc : [...acc, x], [])
    };
  }
}
__name(parseTupleDef, "parseTupleDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/undefined.js
function parseUndefinedDef() {
  return {
    not: {}
  };
}
__name(parseUndefinedDef, "parseUndefinedDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/unknown.js
function parseUnknownDef() {
  return {};
}
__name(parseUnknownDef, "parseUnknownDef");

// node_modules/zod-to-json-schema/dist/esm/parsers/readonly.js
var parseReadonlyDef = /* @__PURE__ */ __name((def, refs) => {
  return parseDef(def.innerType._def, refs);
}, "parseReadonlyDef");

// node_modules/zod-to-json-schema/dist/esm/selectParser.js
var selectParser = /* @__PURE__ */ __name((def, typeName, refs) => {
  switch (typeName) {
    case ZodFirstPartyTypeKind.ZodString:
      return parseStringDef(def, refs);
    case ZodFirstPartyTypeKind.ZodNumber:
      return parseNumberDef(def, refs);
    case ZodFirstPartyTypeKind.ZodObject:
      return parseObjectDef(def, refs);
    case ZodFirstPartyTypeKind.ZodBigInt:
      return parseBigintDef(def, refs);
    case ZodFirstPartyTypeKind.ZodBoolean:
      return parseBooleanDef();
    case ZodFirstPartyTypeKind.ZodDate:
      return parseDateDef(def, refs);
    case ZodFirstPartyTypeKind.ZodUndefined:
      return parseUndefinedDef();
    case ZodFirstPartyTypeKind.ZodNull:
      return parseNullDef(refs);
    case ZodFirstPartyTypeKind.ZodArray:
      return parseArrayDef(def, refs);
    case ZodFirstPartyTypeKind.ZodUnion:
    case ZodFirstPartyTypeKind.ZodDiscriminatedUnion:
      return parseUnionDef(def, refs);
    case ZodFirstPartyTypeKind.ZodIntersection:
      return parseIntersectionDef(def, refs);
    case ZodFirstPartyTypeKind.ZodTuple:
      return parseTupleDef(def, refs);
    case ZodFirstPartyTypeKind.ZodRecord:
      return parseRecordDef(def, refs);
    case ZodFirstPartyTypeKind.ZodLiteral:
      return parseLiteralDef(def, refs);
    case ZodFirstPartyTypeKind.ZodEnum:
      return parseEnumDef(def);
    case ZodFirstPartyTypeKind.ZodNativeEnum:
      return parseNativeEnumDef(def);
    case ZodFirstPartyTypeKind.ZodNullable:
      return parseNullableDef(def, refs);
    case ZodFirstPartyTypeKind.ZodOptional:
      return parseOptionalDef(def, refs);
    case ZodFirstPartyTypeKind.ZodMap:
      return parseMapDef(def, refs);
    case ZodFirstPartyTypeKind.ZodSet:
      return parseSetDef(def, refs);
    case ZodFirstPartyTypeKind.ZodLazy:
      return () => def.getter()._def;
    case ZodFirstPartyTypeKind.ZodPromise:
      return parsePromiseDef(def, refs);
    case ZodFirstPartyTypeKind.ZodNaN:
    case ZodFirstPartyTypeKind.ZodNever:
      return parseNeverDef();
    case ZodFirstPartyTypeKind.ZodEffects:
      return parseEffectsDef(def, refs);
    case ZodFirstPartyTypeKind.ZodAny:
      return parseAnyDef();
    case ZodFirstPartyTypeKind.ZodUnknown:
      return parseUnknownDef();
    case ZodFirstPartyTypeKind.ZodDefault:
      return parseDefaultDef(def, refs);
    case ZodFirstPartyTypeKind.ZodBranded:
      return parseBrandedDef(def, refs);
    case ZodFirstPartyTypeKind.ZodReadonly:
      return parseReadonlyDef(def, refs);
    case ZodFirstPartyTypeKind.ZodCatch:
      return parseCatchDef(def, refs);
    case ZodFirstPartyTypeKind.ZodPipeline:
      return parsePipelineDef(def, refs);
    case ZodFirstPartyTypeKind.ZodFunction:
    case ZodFirstPartyTypeKind.ZodVoid:
    case ZodFirstPartyTypeKind.ZodSymbol:
      return void 0;
    default:
      return /* @__PURE__ */ ((_) => void 0)(typeName);
  }
}, "selectParser");

// node_modules/zod-to-json-schema/dist/esm/parseDef.js
function parseDef(def, refs, forceResolution = false) {
  const seenItem = refs.seen.get(def);
  if (refs.override) {
    const overrideResult = refs.override?.(def, refs, seenItem, forceResolution);
    if (overrideResult !== ignoreOverride) {
      return overrideResult;
    }
  }
  if (seenItem && !forceResolution) {
    const seenSchema = get$ref(seenItem, refs);
    if (seenSchema !== void 0) {
      return seenSchema;
    }
  }
  const newItem = { def, path: refs.currentPath, jsonSchema: void 0 };
  refs.seen.set(def, newItem);
  const jsonSchemaOrGetter = selectParser(def, def.typeName, refs);
  const jsonSchema = typeof jsonSchemaOrGetter === "function" ? parseDef(jsonSchemaOrGetter(), refs) : jsonSchemaOrGetter;
  if (jsonSchema) {
    addMeta(def, refs, jsonSchema);
  }
  if (refs.postProcess) {
    const postProcessResult = refs.postProcess(jsonSchema, def, refs);
    newItem.jsonSchema = jsonSchema;
    return postProcessResult;
  }
  newItem.jsonSchema = jsonSchema;
  return jsonSchema;
}
__name(parseDef, "parseDef");
var get$ref = /* @__PURE__ */ __name((item, refs) => {
  switch (refs.$refStrategy) {
    case "root":
      return { $ref: item.path.join("/") };
    case "relative":
      return { $ref: getRelativePath(refs.currentPath, item.path) };
    case "none":
    case "seen": {
      if (item.path.length < refs.currentPath.length && item.path.every((value, index) => refs.currentPath[index] === value)) {
        console.warn(`Recursive reference detected at ${refs.currentPath.join("/")}! Defaulting to any`);
        return {};
      }
      return refs.$refStrategy === "seen" ? {} : void 0;
    }
  }
}, "get$ref");
var getRelativePath = /* @__PURE__ */ __name((pathA, pathB) => {
  let i = 0;
  for (; i < pathA.length && i < pathB.length; i++) {
    if (pathA[i] !== pathB[i])
      break;
  }
  return [(pathA.length - i).toString(), ...pathB.slice(i)].join("/");
}, "getRelativePath");
var addMeta = /* @__PURE__ */ __name((def, refs, jsonSchema) => {
  if (def.description) {
    jsonSchema.description = def.description;
    if (refs.markdownDescription) {
      jsonSchema.markdownDescription = def.description;
    }
  }
  return jsonSchema;
}, "addMeta");

// node_modules/zod-to-json-schema/dist/esm/zodToJsonSchema.js
var zodToJsonSchema = /* @__PURE__ */ __name((schema, options) => {
  const refs = getRefs(options);
  const definitions = typeof options === "object" && options.definitions ? Object.entries(options.definitions).reduce((acc, [name2, schema2]) => ({
    ...acc,
    [name2]: parseDef(schema2._def, {
      ...refs,
      currentPath: [...refs.basePath, refs.definitionPath, name2]
    }, true) ?? {}
  }), {}) : void 0;
  const name = typeof options === "string" ? options : options?.nameStrategy === "title" ? void 0 : options?.name;
  const main = parseDef(schema._def, name === void 0 ? refs : {
    ...refs,
    currentPath: [...refs.basePath, refs.definitionPath, name]
  }, false) ?? {};
  const title2 = typeof options === "object" && options.name !== void 0 && options.nameStrategy === "title" ? options.name : void 0;
  if (title2 !== void 0) {
    main.title = title2;
  }
  const combined = name === void 0 ? definitions ? {
    ...main,
    [refs.definitionPath]: definitions
  } : main : {
    $ref: [
      ...refs.$refStrategy === "relative" ? [] : refs.basePath,
      refs.definitionPath,
      name
    ].join("/"),
    [refs.definitionPath]: {
      ...definitions,
      [name]: main
    }
  };
  if (refs.target === "jsonSchema7") {
    combined.$schema = "http://json-schema.org/draft-07/schema#";
  } else if (refs.target === "jsonSchema2019-09" || refs.target === "openAi") {
    combined.$schema = "https://json-schema.org/draft/2019-09/schema#";
  }
  if (refs.target === "openAi" && ("anyOf" in combined || "oneOf" in combined || "allOf" in combined || "type" in combined && Array.isArray(combined.type))) {
    console.warn("Warning: OpenAI may not support schemas with unions as roots! Try wrapping it in an object property.");
  }
  return combined;
}, "zodToJsonSchema");

// src/index.ts
var OpenTargetsMCPServer = class {
  static {
    __name(this, "OpenTargetsMCPServer");
  }
  constructor() {
    this.server = new Server(
      {
        name: "open-targets-mcp-server",
        version: "1.0.0"
      },
      {
        capabilities: {
          tools: {}
        }
      }
    );
    this.setupToolHandlers();
  }
  setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: "opentargets_graphql_query",
            description: 'Executes GraphQL queries against the Open Targets Platform API, processes responses into SQLite tables, and returns metadata for subsequent SQL querying.\n\n**Two-Phase Workflow:**\n1. **Data Staging**: This tool executes your GraphQL query and automatically converts the response into normalized SQLite tables\n2. **SQL Analysis**: Use the returned data_access_id with the SQL query tool to perform complex analytical queries\n\n**Open Targets Platform Overview:**\nThe Open Targets Platform integrates evidence from genetics, genomics, transcriptomics, drugs, animal models and scientific literature to score and rank target-disease associations for drug discovery.\n\n**Key API Entities:**\n- **target**: Gene/protein targets (by Ensembl ID) - genetic constraints, tractability, expression\n- **disease**: Diseases/phenotypes (by EFO ID) - ontology, known drugs, clinical signs  \n- **drug**: Compounds/drugs (by ChEMBL ID) - mechanisms of action, indications\n- **associationsOnTheFly**: Target-disease associations with evidence scores\n- **search**: Cross-entity search functionality\n\n**GraphQL Introspection (Recommended First Step):**\nBefore building complex queries, explore the schema:\n```graphql\n{ __schema { types { name kind } } }\n{ __type(name: "Target") { fields { name type { name } } } }\n```\n\n**Example Target Query:**\n```graphql\nquery {\n  target(ensemblId: "ENSG00000169083") {\n    id\n    approvedSymbol\n    biotype\n    geneticConstraint {\n      constraintType\n      exp\n      obs\n      score\n    }\n    tractability {\n      label\n      modality\n      value\n    }\n  }\n}\n```\n\n**Example Disease-Target Associations:**\n```graphql\nquery {\n  disease(efoId: "EFO_0000270") {\n    id\n    name\n    associatedTargets(first: 10) {\n      rows {\n        target {\n          id\n          approvedSymbol\n        }\n        score\n        datatypeScores {\n          datatype\n          score\n        }\n      }\n    }\n  }\n}\n```\n\nReturns a data_access_id for subsequent SQL querying of the staged data.',
            inputSchema: zodToJsonSchema(
              z.object({
                query: z.string().describe("GraphQL query string to execute against Open Targets Platform API"),
                variables: z.record(z.any()).optional().describe("Optional variables for the GraphQL query")
              })
            )
          },
          {
            name: "opentargets_query_sql",
            description: "Execute read-only SQL queries against staged Open Targets data. Use the data_access_id from opentargets_graphql_query to query the SQLite tables created from GraphQL responses.",
            inputSchema: zodToJsonSchema(
              z.object({
                data_access_id: z.string().describe("Data access ID from the GraphQL query tool"),
                sql: z.string().describe("SQL SELECT query to execute against the staged Open Targets data"),
                params: z.array(z.string()).optional().describe("Optional query parameters")
              })
            )
          }
        ]
      };
    });
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;
      try {
        switch (name) {
          case "opentargets_graphql_query":
            return await this.handleGraphQLQuery(args);
          case "opentargets_query_sql":
            return await this.handleSQLQuery(args);
          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error3) {
        return {
          content: [
            {
              type: "text",
              text: `Error: ${error3 instanceof Error ? error3.message : String(error3)}`
            }
          ]
        };
      }
    });
  }
  async handleGraphQLQuery(args) {
    try {
      const response = await fetch("https://api.platform.opentargets.org/api/v4/graphql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          query: args.query,
          variables: args.variables || {}
        })
      });
      if (!response.ok) {
        throw new Error(`Open Targets API error: ${response.status} ${response.statusText}`);
      }
      const data = await response.json();
      if (data.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(data.errors)}`);
      }
      const dataAccessId = `ot_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const result = this.processDataLocally(data, dataAccessId);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              success: true,
              data_access_id: dataAccessId,
              message: "Open Targets GraphQL query executed and data staged successfully",
              ...result || {},
              usage_instructions: [
                `Use data_access_id="${dataAccessId}" with the opentargets_query_sql tool to analyze the staged data`,
                "Example: SELECT * FROM target LIMIT 10;",
                "Example: SELECT t.approved_symbol, d.name FROM target t JOIN target_disease td ON t.id = td.target_id JOIN disease d ON td.disease_id = d.id;",
                "Use SQLite JSON functions for complex analysis: json_extract(column, '$.field')"
              ]
            }, null, 2)
          }
        ]
      };
    } catch (error3) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              success: false,
              error: error3 instanceof Error ? error3.message : String(error3),
              suggestion: "Check your GraphQL query syntax and ensure the Open Targets API is accessible"
            }, null, 2)
          }
        ]
      };
    }
  }
  processDataLocally(data, dataAccessId) {
    const analyzeData = /* @__PURE__ */ __name((obj, path = "") => {
      if (obj === null || obj === void 0) return { type: "null", path };
      if (typeof obj !== "object") return { type: typeof obj, path, sample: obj };
      if (Array.isArray(obj)) {
        return {
          type: "array",
          path,
          length: obj.length,
          sample_elements: obj.slice(0, 3).map((item, i) => analyzeData(item, `${path}[${i}]`))
        };
      }
      const analysis2 = { type: "object", path, fields: {} };
      Object.keys(obj).slice(0, 10).forEach((key) => {
        analysis2.fields[key] = analyzeData(obj[key], `${path}.${key}`);
      });
      return analysis2;
    }, "analyzeData");
    const analysis = analyzeData(data);
    return {
      processing_status: "completed_locally",
      data_structure: analysis,
      suggested_queries: [
        "-- This is a local analysis mode. In production, data would be staged in SQLite tables",
        "-- To enable full SQL querying, deploy to Cloudflare Workers with Durable Objects",
        "-- The data structure above shows what fields are available for analysis"
      ],
      table_count: 1,
      total_rows: Array.isArray(data?.data) ? data.data.length : 1
    };
  }
  async handleSQLQuery(args) {
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify({
            success: false,
            error: "SQL querying requires Cloudflare Workers deployment with Durable Objects",
            data_access_id: args.data_access_id,
            query: args.sql,
            suggestion: "This server is running in local mode. Deploy to Cloudflare Workers to enable full SQL capabilities.",
            local_analysis: "Use the data structure from the GraphQL query response to understand available fields"
          }, null, 2)
        }
      ]
    };
  }
  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error("Open Targets MCP Server running on stdio");
  }
};
var src_default = {
  async fetch(request, env2, ctx) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      const durableObjectId = env2.JSON_TO_SQL_DO.idFromName("default");
      const durableObject = env2.JSON_TO_SQL_DO.get(durableObjectId);
      return durableObject.fetch(request);
    }
    return new Response(JSON.stringify({
      name: "Open Targets MCP Server",
      version: "1.0.0",
      description: "GraphQL to SQL data processing for Open Targets Platform",
      endpoints: {
        "/api/process": "POST - Process GraphQL data into SQL tables",
        "/api/query": "POST - Execute SQL queries on processed data",
        "/api/schema": "GET - Get database schema information"
      }
    }), {
      headers: { "Content-Type": "application/json" }
    });
  }
};
if (typeof globalThis !== "undefined" && "process" in globalThis) {
  const server = new OpenTargetsMCPServer();
  server.run().catch(console.error);
}

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env2, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env2);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env2, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env2);
  } catch (e) {
    const error3 = reduceError(e);
    return Response.json(error3, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-aKJtGZ/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = src_default;

// ../.nvm/versions/node/v20.18.0/lib/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env2, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env2, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env2, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env2, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-aKJtGZ/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env2, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env2, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env2, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env2, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env2, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env2, ctx) => {
      this.env = env2;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  JsonToSqlDO,
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=index.js.map
