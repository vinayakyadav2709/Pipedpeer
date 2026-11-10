"""Notebook kernel driver.

Runs cells in one shared namespace for the lifetime of the process. The parent
talks to it over two inherited pipes, never over stdin/stdout, so nothing user
code prints can be mistaken for protocol:

    control fd  parent -> driver   one JSON object per line
    event fd    driver -> parent   one JSON object per line

File descriptors 1 and 2 are replaced with pipes read by a thread here, so
output is captured at the descriptor level: C extensions and child processes
that write to stdout/stderr are streamed the same way as print().

Only the standard library is required. jedi and matplotlib are used when they
are importable.
"""

import ast
import base64
import builtins
import codecs
import inspect
import io
import json
import linecache
import os
import queue
import re
import select
import signal
import subprocess
import sys
import threading
import time
import traceback
import types

READ_CHUNK = 65536
# Pause after output becomes readable so a burst of small writes is forwarded
# as one event instead of one event per line.
COALESCE_SECONDS = 0.004
REPR_LIMIT = 200
MAX_COMPLETIONS = 200
MAX_VARS = 500

RED = "\x1b[31m"
BOLD_RED = "\x1b[1;31m"
CYAN = "\x1b[36m"
RESET = "\x1b[0m"

DRIVER_FILE = os.path.abspath(__file__)
_IMPORT_PREFIX = re.compile(r"^\s*(?:import|from)\s+([A-Za-z_][A-Za-z0-9_]*)?$")


class Interrupts:
    """Turns SIGINT into KeyboardInterrupt only while user code is running.

    An interrupt that lands between cells would otherwise surface inside the
    driver's own bookkeeping and take the kernel down with it. shield() defers
    one that lands while the main thread is part-way through writing an event,
    which would leave half a line on the event pipe.
    """

    def __init__(self):
        self.user_code = False
        self._shields = 0
        self._pending = False
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame):
        if not self.user_code:
            return
        if self._shields:
            self._pending = True
            return
        raise KeyboardInterrupt

    def shield(self):
        return _Shield(self)


class _Shield:
    def __init__(self, interrupts):
        self._interrupts = interrupts

    def __enter__(self):
        self._interrupts._shields += 1

    def __exit__(self, *exc_info):
        interrupts = self._interrupts
        interrupts._shields -= 1
        if interrupts._shields == 0 and interrupts._pending:
            interrupts._pending = False
            if interrupts.user_code and exc_info[0] is None:
                raise KeyboardInterrupt
        return False


def leave():
    """Ends the kernel at once, with any processes its cells started.

    A cell may be running on the main thread, so nothing is unwound. The
    parent starts the kernel as the leader of its own process group; when the
    parent is gone this is the only thing left that can clean the group up.
    """
    if os.getpgrp() == os.getpid():
        try:
            os.killpg(os.getpgrp(), signal.SIGKILL)
        except OSError:
            pass
    os._exit(0)


class Events:
    """Serialises events onto the event pipe."""

    def __init__(self, fd):
        self._out = os.fdopen(fd, "wb", buffering=0)
        self._lock = threading.Lock()

    def send(self, event):
        try:
            text = json.dumps(event, default=repr, allow_nan=False)
        except ValueError:
            # NaN and Infinity are not JSON; the parent would reject the line.
            text = json.dumps(json.loads(json.dumps(event, default=repr),
                                         parse_constant=lambda name: None))
        line = text.encode("utf-8", errors="replace") + b"\n"
        with self._lock:
            try:
                view = memoryview(line)
                while view:
                    view = view[self._out.write(view):]
            except (BrokenPipeError, OSError):
                leave()


class OutputCapture:
    """Owns fds 1 and 2 and forwards whatever is written to them."""

    def __init__(self, events):
        self._events = events
        self._lock = threading.Lock()
        self._cell = None
        self._streams = {}
        for name, fd in (("stdout", 1), ("stderr", 2)):
            read_fd, write_fd = os.pipe()
            os.dup2(write_fd, fd)
            os.close(write_fd)
            os.set_blocking(read_fd, False)
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            self._streams[read_fd] = (name, decoder)
        sys.stdout = sys.__stdout__ = self._text_stream(1)
        sys.stderr = sys.__stderr__ = self._text_stream(2)
        threading.Thread(target=self._pump, name="pp-output", daemon=True).start()

    @staticmethod
    def _text_stream(fd):
        raw = open(fd, "wb", buffering=0, closefd=False)
        return io.TextIOWrapper(raw, encoding="utf-8", errors="backslashreplace",
                                line_buffering=True, write_through=True)

    def set_cell(self, cell_id):
        with self._lock:
            self._cell = cell_id

    def _pump(self):
        fds = list(self._streams)
        while True:
            try:
                select.select(fds, [], [])
            except (OSError, ValueError):
                return
            time.sleep(COALESCE_SECONDS)
            with self._lock:
                self._forward_ready()

    def _forward_ready(self):
        """Reads everything currently in the pipes. Caller holds the lock.

        Every read happens under the lock, so once drain() returns nothing
        written before it is still on its way to the parent.
        """
        while True:
            ready, _, _ = select.select(list(self._streams), [], [], 0)
            if not ready:
                return
            for fd in ready:
                try:
                    data = os.read(fd, READ_CHUNK)
                except BlockingIOError:
                    continue
                if not data:
                    continue
                name, decoder = self._streams[fd]
                text = decoder.decode(data)
                if text:
                    self._events.send(
                        {"type": "stream", "id": self._cell, "name": name, "text": text})

    def drain(self):
        """Forwards all output written so far, in order, before returning."""
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        with self._lock:
            self._forward_ready()


def mime_bundle(value):
    """Builds a Jupyter mime bundle for value. text/plain is always present."""
    try:
        plain = repr(value)
    except Exception as exc:
        plain = "<repr failed: %s: %s>" % (type(exc).__name__, exc)
    data = {"text/plain": plain}

    bundle_fn = _repr_method(value, "_repr_mimebundle_")
    if bundle_fn is not None:
        try:
            bundle = bundle_fn()
            if isinstance(bundle, tuple):
                bundle = bundle[0]
            if isinstance(bundle, dict):
                for key, val in bundle.items():
                    data[key] = _encode_mime(key, val)
        except Exception:
            pass

    for method, mime in (
        ("_repr_html_", "text/html"),
        ("_repr_markdown_", "text/markdown"),
        ("_repr_latex_", "text/latex"),
        ("_repr_svg_", "image/svg+xml"),
        ("_repr_png_", "image/png"),
        ("_repr_jpeg_", "image/jpeg"),
        ("_repr_json_", "application/json"),
    ):
        if mime in data:
            continue
        fn = _repr_method(value, method)
        if fn is None:
            continue
        try:
            rendered = fn()
        except Exception:
            continue
        if isinstance(rendered, tuple):
            rendered = rendered[0]
        if rendered is None:
            continue
        data[mime] = _encode_mime(mime, rendered)
    return data


def _repr_method(value, name):
    # Classes define these as instance methods; calling one on the class
    # itself would fail for want of self.
    if isinstance(value, type):
        return None
    try:
        fn = getattr(value, name, None)
    except Exception:
        return None
    return fn if callable(fn) else None


def _encode_mime(mime, rendered):
    if isinstance(rendered, (bytes, bytearray)):
        if mime.startswith("image/") and mime != "image/svg+xml":
            return base64.b64encode(bytes(rendered)).decode("ascii")
        return bytes(rendered).decode("utf-8", errors="replace")
    return rendered


def format_error(exc):
    """Returns (ename, evalue, traceback lines) showing user frames only."""
    try:
        summary = traceback.TracebackException.from_exception(exc)
        _drop_driver_frames(summary, set())
        text = "".join(summary.format())
    except Exception:
        text = "%s: %s\n" % (type(exc).__name__, exc)
    lines = text.rstrip("\n").split("\n")

    ename = type(exc).__name__
    try:
        evalue = str(exc)
    except Exception:
        evalue = ""

    qualified = type(exc).__module__ + "." + ename
    colored = []
    for line in lines:
        if line.startswith("Traceback (most recent call last)"):
            colored.append(RED + line + RESET)
        elif line.startswith("  File "):
            colored.append(CYAN + line + RESET)
        elif line.startswith(ename) or line.startswith(qualified):
            colored.append(BOLD_RED + line + RESET)
        else:
            colored.append(line)
    return ename, evalue, colored


def _drop_driver_frames(summary, seen):
    """Removes this file's frames from a TracebackException and everything
    chained to it: the frames that ran the cell, and the signal handler an
    interrupt is raised from."""
    if summary is None or id(summary) in seen:
        return
    seen.add(id(summary))
    summary.stack[:] = [frame for frame in summary.stack if frame.filename != DRIVER_FILE]
    _drop_driver_frames(summary.__cause__, seen)
    _drop_driver_frames(summary.__context__, seen)
    for nested in getattr(summary, "exceptions", None) or ():
        _drop_driver_frames(nested, seen)


def rewrite_shell_lines(source):
    """Rewrites `!cmd`, `%pip ...` and `%time stmt` lines into Python calls.

    Each line maps to exactly one line, so tracebacks keep their line numbers.
    Only called for source that is not valid Python as written.
    """
    out = []
    for line in source.split("\n"):
        stripped = line.lstrip()
        indent = line[: len(line) - len(stripped)]
        if stripped.startswith("!"):
            out.append("%s_pp_shell(%r)" % (indent, stripped[1:].strip()))
        elif stripped == "%pip" or stripped.startswith("%pip "):
            out.append("%s_pp_pip(%r)" % (indent, stripped[4:].strip()))
        elif stripped.startswith("%time "):
            out.append("%s_pp_time(%r)" % (indent, stripped[6:].strip()))
        else:
            out.append(line)
    return "\n".join(out)


class Kernel:
    def __init__(self, events, capture, interrupts):
        self.events = events
        self.capture = capture
        self.interrupts = interrupts
        self.cell = None
        self.loop = None
        self.completer = "rlcompleter"
        try:
            import jedi  # noqa: F401
            self.completer = "jedi"
        except Exception:
            pass

        main = types.ModuleType("__main__")
        main.__dict__["__builtins__"] = builtins
        # User classes and functions live in __main__, as they would in a
        # script, so pickling and multiprocessing find them there.
        sys.modules["__main__"] = main
        self.ns = main.__dict__
        self.seed = set()
        builtins.display = self.display
        self.ns.update({
            "_pp_shell": self.shell,
            "_pp_pip": self.pip,
            "_pp_time": self.timed,
        })
        self.seed = set(self.ns)
        sys.meta_path.insert(0, _PyplotHook(self.flush_figures))

    # ---- output ----

    def emit(self, event):
        """Sends an event after the output written before it."""
        with self.interrupts.shield():
            self.capture.drain()
            self.events.send(event)

    def display(self, *values, display_id=None):
        """Shows each value in the cell's output using its richest repr."""
        for value in values:
            event = {"type": "display", "id": self.cell, "data": mime_bundle(value)}
            if display_id is not None:
                event["display_id"] = str(display_id)
            self.emit(event)

    def flush_figures(self):
        """Emits every open matplotlib figure as a PNG and closes it."""
        plt = sys.modules.get("matplotlib.pyplot")
        if plt is None:
            return
        try:
            numbers = plt.get_fignums()
        except Exception:
            return
        for number in numbers:
            try:
                figure = plt.figure(number)
                buffer = io.BytesIO()
                figure.savefig(buffer, format="png", bbox_inches="tight")
                data = {
                    "text/plain": repr(figure),
                    "image/png": base64.b64encode(buffer.getvalue()).decode("ascii"),
                }
                self.emit({"type": "display", "id": self.cell, "data": data})
            except Exception:
                traceback.print_exc()
        try:
            plt.close("all")
        except Exception:
            pass

    # ---- shell helpers ----

    def shell(self, command):
        sys.stdout.flush()
        sys.stderr.flush()
        code = subprocess.call(command, shell=True, stdin=subprocess.DEVNULL)
        self.ns["_exit_code"] = code

    def pip(self, arguments):
        self.shell('"%s" -m pip %s' % (sys.executable, arguments))

    def timed(self, statement):
        wall = time.perf_counter()
        cpu = time.process_time()
        try:
            code = compile(statement, "<%time>", "eval")
        except SyntaxError:
            exec(compile(statement, "<%time>", "exec"), self.ns)
            value = None
        else:
            value = eval(code, self.ns)
        cpu = time.process_time() - cpu
        wall = time.perf_counter() - wall
        print("CPU time: %s\nWall time: %s" % (_duration(cpu), _duration(wall)))
        return value

    # ---- execution ----

    def execute(self, request):
        cell = request["id"]
        count = request.get("execution_count", 0)
        source = request.get("code", "")
        filename = "<cell %d>" % count
        self.cell = cell
        self.capture.set_cell(cell)

        status = "ok"
        try:
            self.interrupts.user_code = True
            try:
                self.run_source(source, filename, cell, count)
            finally:
                self.interrupts.user_code = False
        except BaseException as exc:
            self.interrupts.user_code = False
            status = "error"
            ename, evalue, lines = format_error(exc)
            self.emit({"type": "error", "id": cell, "ename": ename,
                       "evalue": evalue, "traceback": lines})

        self.flush_figures()
        self.emit({"type": "done", "id": cell, "execution_count": count,
                   "status": status})

    def run_source(self, source, filename, cell, count):
        try:
            tree = ast.parse(source, filename, "exec")
        except SyntaxError:
            rewritten = rewrite_shell_lines(source)
            if rewritten == source:
                self.remember_source(filename, source)
                raise
            tree = ast.parse(rewritten, filename, "exec")
        self.remember_source(filename, source)

        last = None
        tail = tree.body[-1] if tree.body else None
        if isinstance(tail, ast.Expr) and not source.rstrip().endswith(";"):
            tree.body.pop()
            last = ast.Expression(tail.value)

        flags = ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
        if tree.body:
            self.run_code(compile(tree, filename, "exec", flags=flags))
        if last is None:
            return
        ast.fix_missing_locations(last)
        value = self.run_code(compile(last, filename, "eval", flags=flags))
        if value is None:
            return
        self.ns["_"] = value
        self.emit({"type": "result", "id": cell, "execution_count": count,
                   "data": mime_bundle(value)})

    def run_code(self, code):
        if code.co_flags & inspect.CO_COROUTINE:
            import asyncio
            if self.loop is None:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            return self.loop.run_until_complete(eval(code, self.ns))
        return eval(code, self.ns)

    def remember_source(self, filename, source):
        # mtime None marks the entry as not backed by a file, which keeps
        # linecache.checkcache from discarding it.
        lines = [line + "\n" for line in source.split("\n")]
        linecache.cache[filename] = (len(source), None, lines, filename)

    # ---- introspection ----

    def complete(self, request):
        code = request.get("code", "")
        cursor = request.get("cursor", len(code))
        cursor = max(0, min(cursor, len(code)))
        matches, start = [], cursor
        try:
            matches, start = self.find_completions(code, cursor)
        except Exception:
            pass
        self.events.send({"type": "complete_reply", "req_id": request.get("req_id"),
                          "matches": matches[:MAX_COMPLETIONS],
                          "cursor_start": start, "cursor_end": cursor})

    def find_completions(self, code, cursor):
        if self.completer == "jedi":
            import jedi
            before = code[:cursor]
            line = before.count("\n") + 1
            column = len(before) - (before.rfind("\n") + 1)
            found = jedi.Interpreter(code, [self.ns]).complete(line, column)
            if found:
                prefix = found[0].get_completion_prefix_length()
                return [c.name for c in found], cursor - prefix

        before = code[:cursor]
        current_line = before[before.rfind("\n") + 1:]
        importing = _IMPORT_PREFIX.match(current_line)
        if importing:
            prefix = importing.group(1) or ""
            return self.module_names(prefix), cursor - len(prefix)

        end = len(before)
        begin = end
        while begin > 0 and (before[begin - 1].isalnum() or before[begin - 1] in "_."):
            begin -= 1
        token = before[begin:end]
        if not token:
            return [], cursor

        import rlcompleter
        completer = rlcompleter.Completer(self.ns)
        seen, matches = set(), []
        for index in range(MAX_COMPLETIONS * 4):
            candidate = completer.complete(token, index)
            if candidate is None:
                break
            candidate = candidate.rstrip("( ").rstrip(":")
            if candidate and candidate not in seen:
                seen.add(candidate)
                matches.append(candidate)
        # rlcompleter returns whole dotted paths; the editor replaces only the
        # last component.
        if "." in token:
            head = token.rsplit(".", 1)[0] + "."
            matches = [m[len(head):] if m.startswith(head) else m for m in matches]
            begin = begin + len(head)
        return sorted(matches, key=lambda m: (m.startswith("_"), m)), begin

    def module_names(self, prefix):
        import pkgutil
        names = set(sys.builtin_module_names)
        for module in pkgutil.iter_modules():
            names.add(module.name)
        return sorted(n for n in names if n.startswith(prefix) and not n.startswith("_"))

    def inspect_vars(self, request):
        rows = []
        try:
            items = list(self.ns.items())
        except RuntimeError:
            items = []
        for name, value in items:
            if name.startswith("_") or name in self.seed:
                continue
            if isinstance(value, (types.ModuleType, types.FunctionType,
                                  types.BuiltinFunctionType, type)):
                continue
            rows.append({
                "name": name,
                "type": type(value).__name__,
                "repr": _short_repr(value),
                "size_bytes": _size_of(value),
            })
            if len(rows) >= MAX_VARS:
                break
        rows.sort(key=lambda row: row["name"])
        self.events.send({"type": "vars_reply", "req_id": request.get("req_id"),
                          "vars": rows})


class _PyplotHook:
    """Replaces pyplot.show the moment pyplot is imported.

    Under the non-interactive Agg backend show() only warns. Emitting the
    figures instead makes the usual `plt.plot(...); plt.show()` cell work, and
    doing it at import time covers the first cell that uses pyplot.
    """

    def __init__(self, flush):
        self._flush = flush

    def find_spec(self, name, path=None, target=None):
        if name != "matplotlib.pyplot":
            return None
        sys.meta_path.remove(self)
        import importlib.util
        spec = importlib.util.find_spec(name)
        if spec is None or spec.loader is None:
            return spec
        load = spec.loader.exec_module
        flush = self._flush

        # A plain function, because pyplot assigns attributes to its show.
        def show(*args, **kwargs):
            flush()

        def exec_module(module):
            load(module)
            module.show = show

        spec.loader.exec_module = exec_module
        return spec


def use_fork_for_multiprocessing():
    """Makes functions defined in cells usable with multiprocessing.

    Cells have no file to re-import, so the spawn and forkserver start methods
    cannot find a cell's functions in a worker. fork copies the namespace.
    """
    if not sys.platform.startswith("linux"):
        return
    import multiprocessing
    if "fork" in multiprocessing.get_all_start_methods():
        multiprocessing.set_start_method("fork")


def _duration(seconds):
    if seconds >= 1:
        return "%.2f s" % seconds
    if seconds >= 1e-3:
        return "%.2f ms" % (seconds * 1e3)
    return "%.2f us" % (seconds * 1e6)


def _short_repr(value):
    try:
        shape = getattr(value, "shape", None)
        if isinstance(shape, tuple):
            return str(shape)
        text = repr(value)
    except Exception as exc:
        return "<repr failed: %s>" % type(exc).__name__
    if len(text) > REPR_LIMIT:
        text = text[: REPR_LIMIT - 1] + "…"
    return text


def _size_of(value):
    try:
        nbytes = getattr(value, "nbytes", None)
        if isinstance(nbytes, int):
            return nbytes
        usage = getattr(value, "memory_usage", None)
        if callable(usage):
            total = usage()
            return int(getattr(total, "sum", lambda: total)())
        return int(sys.getsizeof(value))
    except Exception:
        return 0


def read_control(control_fd, kernel, work):
    """Reads parent commands. Introspection is answered here so it works
    while a cell is running; executes are handed to the main thread."""
    with os.fdopen(control_fd, "rb") as control:
        for raw in control:
            try:
                request = json.loads(raw)
            except ValueError:
                continue
            kind = request.get("type")
            if kind == "execute":
                work.put(request)
            elif kind == "complete":
                kernel.complete(request)
            elif kind == "inspect_vars":
                kernel.inspect_vars(request)
            elif kind == "shutdown":
                break
    # The parent closed the pipe or asked us to stop.
    leave()


def main():
    control_fd, event_fd = int(sys.argv[1]), int(sys.argv[2])
    events = Events(event_fd)
    interrupts = Interrupts()
    capture = OutputCapture(events)

    # Python put the driver's own directory first on sys.path; a notebook
    # imports relative to its working directory instead.
    driver_dir = os.path.dirname(DRIVER_FILE)
    sys.path[:] = [""] + [p for p in sys.path if p and os.path.abspath(p) != driver_dir]
    sys.argv = [""]
    use_fork_for_multiprocessing()

    kernel = Kernel(events, capture, interrupts)
    work = queue.Queue()
    threading.Thread(target=read_control, args=(control_fd, kernel, work),
                     name="pp-control", daemon=True).start()

    events.send({"type": "ready", "python": sys.version.split()[0],
                 "pid": os.getpid(), "completer": kernel.completer})
    while True:
        try:
            request = work.get()
        except KeyboardInterrupt:
            continue
        kernel.execute(request)


if __name__ == "__main__":
    main()
