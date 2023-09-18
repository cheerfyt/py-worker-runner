import * as Comlink from "comlink";
import { SyncClient, SyncExtras, syncExpose } from "comsync";
import retry from "p-retry";
import type { PyodideInterface } from "pyodide";
import { loadPyodide } from "pyodide";

const bufferCache = new Map<string, ArrayBuffer>();

const RUNNER_CONTENT = `
import importlib
import sys
from typing import Callable, Literal, Union, TypedDict

try:
    from pyodide.code import find_imports  # noqa
except ImportError:
    from pyodide import find_imports  # noqa

import pyodide_js  # noqa

sys.setrecursionlimit(400)

class InstallEntry(TypedDict):
    module: str
    package: str


def find_imports_to_install(imports: list[str]) -> list[InstallEntry]:
    """
    Given a list of module names being imported, return a list of dicts
    representing the packages that need to be installed to import those modules.
    The returned list will only contain modules that aren't already installed.
    Each returned dict has the following keys:
      - module: the name of the module being imported
      - package: the name of the package that needs to be installed
    """
    try:
        to_package_name = pyodide_js._module._import_name_to_package_name.to_py()
    except AttributeError:
        to_package_name = pyodide_js._api._import_name_to_package_name.to_py()

    to_install: list[InstallEntry] = []
    for module in imports:
        try:
            importlib.import_module(module)
        except ModuleNotFoundError:
            to_install.append(
                dict(
                    module=module,
                    package=to_package_name.get(module, module),
                )
            )
    return to_install


async def install_imports(
    source_code_or_imports: Union[str, list[str]],
    message_callback: Callable[
        [
            Literal[
                "loading_all",
                "loaded_all",
                "loading_one",
                "loaded_one",
                "loading_micropip",
                "loaded_micropip",
            ],
            Union[InstallEntry, list[InstallEntry]],
        ],
        None,
    ] = lambda event_type, data: None,
):
    """
    Accepts a string of Python source code or a list of module names being imported.
    Installs any packages that need to be installed to import those modules,
    using micropip, which may also be installed if needed.
    If the package is not specially built for Pyodide, it must be available on PyPI
    as a pure Python wheel file.
    If the \`message_callback\` argument is provided, it will be called with an
    event type and data about the packages being installed.
    The event types start with \`loading_\` before installation, and \`loaded_\` after.
    The data is either a single dict representing the package being installed,
    or a list of all the packages being installed.
    The events are:
        - loading/loaded_all, with a list of all the packages being installed.
        - loading/loaded_one, with a dict for a single package.
        - loading/loaded_micropip, with a dict for the special micropip package.
    """
    if isinstance(source_code_or_imports, str):
        try:
            imports: list[str] = find_imports(source_code_or_imports)
        except SyntaxError:
            return
    else:
        imports: list[str] = source_code_or_imports

    to_install = find_imports_to_install(imports)
    if to_install:
        message_callback("loading_all", to_install)
        try:
            import micropip  # noqa
        except ModuleNotFoundError:
            micropip_entry = dict(module="micropip", package="micropip")
            message_callback("loading_micropip", micropip_entry)
            await pyodide_js.loadPackage("micropip")
            import micropip  # noqa

            message_callback("loaded_micropip", micropip_entry)

        for entry in to_install:
            message_callback("loading_one", entry)
            await micropip.install(entry["package"])
            message_callback("loaded_one", entry)
        message_callback("loaded_all", to_install)
`;

async function getPkgArrayBuffer(url: string): Promise<ArrayBuffer> {
  if (bufferCache.has(url)) {
    return bufferCache.get(url)!;
  }
  console.log("Fetch package from [%s]", url);
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(
      `Request for package failed with status ${response.status}: ${response.statusText}`
    );
  }
  const buffer = await response.arrayBuffer();
  bufferCache.set(url, buffer);
  return buffer;
}

/**
 * Loads Pyodide in parallel to downloading an archive with your own code and dependencies,
 * which it then unpacks into the virtual filesystem ready for you to import.
 *
 * This helps you to work with Python code normally with several `.py` files
 * instead of a giant string passed to Pyodide's `runPython`.
 * It also lets you start up more quickly instead of waiting for Pyodide to load
 * before calling `loadPackage` or `micropip.install`.
 *
 * The archive should contain your own Python files and any Python dependencies.
 * A simple way to gather Python dependencies into a folder is with `pip install -t <folder>`.
 * The location where the archive is extracted will be added to `sys.path` so it can be imported immediately,
 * e.g. with [`pyodide.pyimport`](https://pyodide.org/en/stable/usage/api/js-api.html#pyodide.pyimport).
 * There should be no top-level folder in the archive containing everything else,
 * or that's what you'll have to import.
 *
 * If you don't use `loadPyodideAndPackage` and just load Pyodide yourself,
 * then we recommend passing the resulting module object to `initPyodide` for some other housekeeping.
 *
 * Loading of both Pyodide and the package is retried up to 3 times in case of network errors.
 *
 * The raw contents of the package are cached in memory to avoid re-downloading in case of
 * a fatal error which requires reloading Pyodide.
 *
 * @param packageOptions Object which describes how to load the package file, with the following keys:
 *    - `url`: URL to fetch the package from.
 *    - `format`: File format which determines how to extract the archive.
 *                By default the options are 'bztar', 'gztar', 'tar', 'zip', and 'wheel'.
 *    - `extractDir`: Directory to extract the archive into. Defaults to /tmp/.
 * @param pyodideLoader Optional function which takes no arguments and returns the Pyodide module as returned by the
 *    [`loadPyodide`](https://pyodide.org/en/stable/usage/api/js-api.html#globalThis.loadPyodide) function.
 *    Defaults to `defaultPyodideLoader`which uses the
 *    [official CDN](https://pyodide.org/en/stable/usage/quickstart.html#setup).
 */
export async function loadPyodideAndPackage(
  options: PackageOptions,
  loader = defaultPyodideLoader
): Promise<PyodideInterface> {
  const { extractDir = "/tmp", url, format } = options;

  let pyodide: PyodideInterface;
  let packageBuffer: ArrayBuffer;

  [pyodide, packageBuffer] = await Promise.all([
    retry(() => loader(), { retries: 3 }),
    retry(() => getPkgArrayBuffer(url), { retries: 3 }),
  ]);

  pyodide.unpackArchive(packageBuffer, format, { extractDir });
  const sys = pyodide.pyimport("sys");
  sys.path.append(extractDir);
  await initPyodide(pyodide);
  return pyodide;
}

/**
 * Initializes the given Pyodide module with some extra functionality:
 *   - `pyodide.registerComlink(Comlink)` makes `Comlink` (and thus `comsync` and the `PyodideClient`) work better.
 *   - Imports the `pyodide_worker_runner` Python module included with this library, which:
 *     - Immediately calls `sys.setrecursionlimit` so that deep recursion causes a Python `RecursionError`
 *       instead of a fatal JS `RangeError: Maximum call stack size exceeded`.
 *     - Provides the `install_imports` function which allows automatically installing imported modules, similar to
 *       [`loadPackagesFromImports`](https://pyodide.org/en/stable/usage/api/js-api.html#pyodide.loadPackagesFromImports)
 *       but also loads packages which are not built into Pyodide but can be installed with `micropip`,
 *       i.e. pure Python packages with wheels available on PyPI.
 */
export function initPyodide(pyodide: PyodideInterface): void {
  pyodide.registerComlink(Comlink);
  const sys = pyodide.pyimport("sys");
  const pathlib = pyodide.pyimport("pathlib");

  const dirPath = "/tmp/py_worker_runner/";
  sys.path.append(dirPath);
  pathlib.Path(dirPath).mkdir();
  pathlib.Path(dirPath + "py_worker_runner.py").write_text(RUNNER_CONTENT);
  pyodide.pyimport("py_worker_runner");
}

/**
 * Construct a callback function which can be passed to `python_runner` to handle events.
 * See https://github.com/alexmojaki/python_runner#callback-events
 *
 * @param comsyncExtras
 *    In a web worker script, you need to pass a function to `pyodideExpose` from this library.
 *    That function will be passed `extras` as its first argument which can then be used here.
 *    `extras` is expected to contain a `channel` created by the `makeChannel` function from
 *    the [`sync-message`](https://github.com/alexmojaki/sync-message) library
 *    which was then passed to `PyodideClient` in the main thread.
 *    This enables synchronous communication between the main thread and the worker,
 *    which is used by the callback to handle `input` and `sleep` events properly.
 *    See https://github.com/alexmojaki/pyodide-worker-runner#comsync-integration for more info.
 * @param callbacks
 *    An object containing callback functions to handle the different event types.
 *      - `output`: Required. Called with an array of output parts,
 *         e.g. `[{type: "stdout", text: "Hello world"}]`.
 *         Use this to tell your UI to display the output.
 *      - `input`: Optional. Called when the Python code reads from `sys.stdin`, e.g. with `input()`.
 *         Use this to tell your UI to wait for the user to enter some text.
 *         The entered text should be passed to `PyodideClient.writeMessage()` in the main thread,
 *         and will be returned synchronously by this function to the Python code.
 *         When the Python code calls `input(prompt)`, the string `prompt` is passed to this callback.
 *         Two types of output part will also be passed to the `output` callback:
 *             - `input_prompt`: the prompt passed to the `input()` function.
 *               Using this output part may be a better way to display the prompt in the UI
 *               than the argument of the `input` callback, but the `input` callback is still needed
 *               even if it doesn't display the prompt.
 *              - `input`: the user's input passed to stdin.
 *                Not actually 'output', but included as an output part
 *                because it's typically shown in regular Python consoles.
 *      - `other`: Optional. Called for all other event types
 *        (except `sleep` which is handled directly by `makeRunnerCallback`).
 *        The actual event type is passed as the first argument.
 */
export function makeRunnerCallable(
  extras: SyncExtras,
  callbacks: RunnerCallbacks
) {
  return function (type: string, data: any) {
    if (data.toJs) {
      data = data.toJs({ dict_converter: Object.fromEntries });
    }
    if (type === "input") {
      callbacks.input && callbacks.input(data.prompt);
      return extras.readMessage() + "\n";
    } else if (type === "sleep") {
      extras.syncSleep(data.seconds * 1000);
    } else if (type === "output") {
      return callbacks.output(data.parts);
    } else {
      return callbacks.other!(type, data);
    }
  };
}

/**
 * This class should be used in the main browser thread
 * to call functions exposed with `pyodideExpose` and `Comlink.expose` in a web worker.
 * See https://github.com/alexmojaki/comsync to learn how to use the base class `SyncClient`.
 * What this class adds is making it easier to interrupt Python code running in Pyodide in the worker.
 * Specifically, if `SharedArrayBuffer` is available, then `PyodideClient.call` will pass a buffer
 * so that the function passed to `pyodideExpose` can call `pyodide.setInterruptBuffer(extras.interruptBuffer)`
 * to enable interruption. Then `PyodideClient.interrupt()` will use the buffer.
 * Otherwise, `PyodideClient.interrupt()` will likely restart the web worker entirely, which is more disruptive.
 */
export class PyodideClient<T = any> extends SyncClient<T> {
  async call(proxyMethod: any, ...args: any[]) {
    let interruptBuffer: Int32Array | null = null;
    if (typeof SharedArrayBuffer !== "undefined") {
      interruptBuffer = new Int32Array(
        new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 1)
      );
      this.interrupter = () => {
        interruptBuffer![0] = 2;
      };
    }

    return super.call(proxyMethod, interruptBuffer, ...args);
  }
}

/**
 * This class handles automatically reloading Pyodide from scratch when a fatal error occurs.
 * The constructor accepts a 'loader' function which should return a promise that resolves to a Pyodide module.
 * We recommend a function which calls `loadPyodideAndPackage`.
 * The loader function will be called immediately on construction.
 *
 * Code that uses the Pyodide module should be wrapped in a `withPyodide` call, e.g:
 *
 *    await pyodideFatalErrorReloader.withPyodide(async (pyodide) => {
 *      pyodide.runCode(...);
 *    });
 *
 * If a fatal error occurs, the loader function will be called again immediately to reload Pyodide in the background,
 * while the error is rethrown for you to handle.
 * The next call to `withPyodide` will then be able to use the new Pyodide instance.
 *
 * In general, `withPyodide` will wait for the loader function to complete, and will throw any errors it throws.
 */
export class PyodideFatalErrorReloader {
  private pyodidePromise: Promise<PyodideInterface>;

  constructor(private readonly loader: PyodideLoader) {
    this.pyodidePromise = loader();
  }

  public async withPyodide<T>(
    fn: (pyodide: PyodideInterface) => Promise<T>
  ): Promise<T> {
    const pyodide = await this.pyodidePromise;
    try {
      return await fn(pyodide);
    } catch (e) {
      if (e.pyodide_fatal_error) {
        this.pyodidePromise = this.loader();
      }
      throw e;
    }
  }
}

type ExposeFunc<T extends any[], R> = (extras: PyodideExtras, ...args: T) => R;

/**
 * Call this in your web worker code with a function `func`
 * to allow it to be called from the main thread by `PyodideClient.call`.
 *
 * `func` will be called with an object `extras` of type `PyodideExtras` as its first argument.
 * The other arguments are those passed to `PyodideClient.call`,
 * and the return value is passed back to the main thread and returned from `PyodideClient.call`.
 *
 * `func` will be wrapped into a new function which is returned here.
 * The returned function should be passed to `Comlink.expose`, possibly as part of a larger object.
 *
 * For example, the worker code may look something like this:
 *
 *     Comlink.expose({
 *       myRunCode: pyodideExpose((extras, code) => {
 *         pyodide.runCode(code);
 *       }),
 *     });
 *
 * and the main thread code may look something like this:
 *
 *    await client.call(client.workerProxy.myRunCode, "print('Hello world')");
 *
 * It's recommended to pass `extras` to `makeRunnerCallback` and then pass the resulting callback
 * to a `python_runner.PyodideRunner` instead of calling `pyodide.runCode` directly.
 *
 * If possible, `extras.interruptBuffer` will be a `SharedArrayBuffer` which can be used like this:
 *
 *     if (extras.interruptBuffer) {
 *       pyodide.setInterruptBuffer(extras.interruptBuffer);
 *     }
 *
 * This will allow the main thread to call `PyodideClient.interrupt()`
 * to raise `KeyboardInterrupt` in Python code running in Pyodide in the worker thread.
 * `setInterruptBuffer` isn't called automatically so that you can choose the correct place to call it,
 * after running any Python code that musn't be interrupted.
 */
export function pyodideExpose<T extends any[], R>(
  func: ExposeFunc<T, R>
): ReturnType<typeof syncExpose> {
  return syncExpose(async function (
    comsyncExtras: SyncExtras,
    interruptBuffer: Int32Array | null,
    ...args: T
  ): Promise<R> {
    return func({ ...comsyncExtras, interruptBuffer }, ...args);
  });
}

export async function defaultPyodideLoader(
  options: PyodideLoadOptions = {}
): Promise<PyodideInterface> {
  const pyodide = await loadPyodide(options);
  return pyodide;
}

export type PyodideLoader = () => Promise<PyodideInterface>;

export interface PyodideExtras extends SyncExtras {
  interruptBuffer: Int32Array | null;
}

export type PkgFormat = "bztar" | "gztar" | "tar" | "zip" | "wheel";

export interface PackageOptions {
  url: string; // URL to fetch the package from
  format: PkgFormat; // (https://pyodide.org/en/stable/usage/api/js-api.html#pyodide.unpackArchive)
  extractDir?: string; // Defaults to /tmp/
}

// from pyodide source code
export interface PyodideLoadOptions {
  /**
   * The URL from which Pyodide will load the main Pyodide runtime and
   * packages. It is recommended that you leave this unchanged, providing an
   * incorrect value can cause broken behavior.
   *
   * Default: The url that Pyodide is loaded from with the file name
   * (``pyodide.js`` or ``pyodide.mjs``) removed.
   */
  indexURL?: string;
  /**
   * The file path where packages will be cached in node. If a package
   * exists in ``packageCacheDir`` it is loaded from there, otherwise it is
   * downloaded from the JsDelivr CDN and then cached into ``packageCacheDir``.
   * Only applies when running in node; ignored in browsers.
   *
   * Default: same as indexURL
   */
  packageCacheDir?: string;
  /**
   * The URL from which Pyodide will load the Pyodide ``pyodide-lock.json`` lock
   * file. You can produce custom lock files with :py:func:`micropip.freeze`.
   * Default: ```${indexURL}/pyodide-lock.json```
   */
  lockFileURL?: string;
  /**
   * The home directory which Pyodide will use inside virtual file system.
   * This is deprecated, use ``{env: {HOME : some_dir}}`` instead.
   */
  homedir?: string;
  /**
   * Load the full Python standard library. Setting this to false excludes
   * unvendored modules from the standard library.
   * Default: ``false``
   */
  fullStdLib?: boolean;
  /**
   * The URL from which to load the standard library ``python_stdlib.zip``
   * file. This URL includes the most of the Python standard library. Some
   * stdlib modules were unvendored, and can be loaded separately
   * with ``fullStdLib: true`` option or by their package name.
   * Default: ```${indexURL}/python_stdlib.zip```
   */
  stdLibURL?: string;
  /**
   * Override the standard input callback. Should ask the user for one line of
   * input. The :js:func:`pyodide.setStdin` function is more flexible and
   * should be preferred.
   */
  stdin?: () => string;
  /**
   * Override the standard output callback. The :js:func:`pyodide.setStdout`
   * function is more flexible and should be preferred in most cases, but
   * depending on the ``args`` passed to ``loadPyodide``, Pyodide may write to
   * stdout on startup, which can only be controlled by passing a custom
   * ``stdout`` function.
   */
  stdout?: (msg: string) => void;
  /**
   * Override the standard error output callback. The
   * :js:func:`pyodide.setStderr` function is more flexible and should be
   * preferred in most cases, but depending on the ``args`` passed to
   * ``loadPyodide``, Pyodide may write to stdout on startup, which can only
   * be controlled by passing a custom ``stdout`` function.
   */
  stderr?: (msg: string) => void;
  /**
   * The object that Pyodide will use for the ``js`` module.
   * Default: ``globalThis``
   */
  jsglobals?: object;
  /**
   * Command line arguments to pass to Python on startup. See `Python command
   * line interface options
   * <https://docs.python.org/3.10/using/cmdline.html#interface-options>`_ for
   * more details. Default: ``[]``
   */
  args?: string[];
  /**
   * Environment variables to pass to Python. This can be accessed inside of
   * Python at runtime via :py:data:`os.environ`. Certain environment variables change
   * the way that Python loads:
   * https://docs.python.org/3.10/using/cmdline.html#environment-variables
   * Default: ``{}``.
   * If ``env.HOME`` is undefined, it will be set to a default value of
   * ``"/home/pyodide"``
   */
  env?: {
    [key: string]: string;
  };
  /**
   * A list of packages to load as Pyodide is initializing.
   *
   * This is the same as loading the packages with
   * :js:func:`pyodide.loadPackage` after Pyodide is loaded except using the
   * ``packages`` option is more efficient because the packages are downloaded
   * while Pyodide bootstraps itself.
   */
  packages?: string[];
  /**
   * @ignore
   */
  _node_mounts?: string[];
}

export interface OutputPart {
  type: string;
  text: string;
  [key: string]: unknown;
}

export interface RunnerCallbacks {
  input?: (prompt: string) => void;
  output: (parts: OutputPart[]) => unknown;
  other?: (type: string, data: unknown) => unknown;
}

export interface PyodideExtras extends SyncExtras {
  interruptBuffer: Int32Array | null;
}
