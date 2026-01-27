from __future__ import annotations

import functools
import inspect
import itertools
import weakref
from typing import Any, ClassVar, cast, get_args, overload

from deprecated.sphinx import versionadded

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.commands.request import CommandRequest, CommandResponseT
from coredis.exceptions import FunctionError
from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    TYPE_CHECKING,
    AnyStr,
    Callable,
    Generator,
    Generic,
    KeyT,
    MutableMapping,
    P,
    Parameters,
    R,
    StringT,
    TypeVar,
    ValueT,
    add_runtime_checks,
    safe_beartype,
)

if TYPE_CHECKING:
    import coredis.client

LibraryT = TypeVar("LibraryT", bound="Library[Any]")
LibraryStringT = TypeVar("LibraryStringT", bound="Library[str]")
LibraryBytesT = TypeVar("LibraryBytesT", bound="Library[bytes]")


class Library(Generic[AnyStr]):
    #: Class variable equivalent of the :paramref:`Library.name` argument.
    NAME: ClassVar[StringT | None] = None
    #: Class variable equivalent of the :paramref:`Library.code` argument.
    CODE: ClassVar[StringT | None] = None

    def __init__(
        self,
        client: coredis.client.Client[AnyStr],
        name: StringT | None = None,
        code: StringT | None = None,
        replace: bool = False,
    ) -> None:
        """
        Abstraction over a library of redis functions

        Example::

            library_code = \"\"\"
            #!lua name=coredis
            redis.register_function('myfunc', function(k, a) return a[1] end)
            \"\"\"
            lib = await Library(client, "mylib", library_code)
            assert "1" == await lib["myfunc"]([], [1])

        When used as a base class the class variables :data:`NAME` and :data:`CODE`
        can be set on the sub class to avoid having to implement a constructor. Constructor
        parameters will take precedence over the class variables.

        :param client: The coredis client instance to use when calling the functions
         exposed by the library.
        :param name: The name of the library (should match the name in the Shebang
         in the library source).
        :param code: The lua code representing the library
        :param replace: Whether to replace the library when intializing. If ``False``
         an exception will be raised if the library was already loaded in the target
         redis instance.
        """
        self._client: weakref.ReferenceType[coredis.client.Client[AnyStr]] = weakref.ref(client)
        self.name = nativestr(name or self.NAME)
        self.code = (code or self.CODE or "").lstrip()
        self._functions: EncodingInsensitiveDict = EncodingInsensitiveDict()
        self.replace = replace
        if self.replace and not self.code:
            raise RuntimeError("library code must be provided when the ``replace`` option is used")

    @property
    def client(self) -> coredis.client.Client[AnyStr]:
        c = self._client()
        assert c
        return c

    @property
    def functions(self) -> MutableMapping[str, Function[AnyStr]]:
        """
        mapping of function names to :class:`~coredis.commands.function.Function`
        instances that can be directly called.
        """
        return self._functions

    async def update(self, new_code: StringT) -> bool:
        """
        Update the code of a library with :paramref:`new_code`
        """
        self.code = new_code
        if await self.initialize(replace=True):
            return True
        return False

    async def initialize(self: LibraryT, replace: bool = False) -> LibraryT:
        from coredis.pipeline import ClusterPipeline, Pipeline

        self._functions.clear()
        if isinstance(self.client, (Pipeline, ClusterPipeline)):
            redis_client = self.client.client
        else:
            redis_client = self.client
        library = (await redis_client.function_list(self.name)).get(self.name)
        if (not library and self.code) or (replace or self.replace):
            await redis_client.function_load(self.code, replace=replace or self.replace)
            library = (await redis_client.function_list(self.name)).get(self.name)

        if not library:
            raise FunctionError(f"No library found for {self.name}")

        for name, details in library["functions"].items():
            self._functions[name] = Function[AnyStr](
                self.client,
                self.name,
                name,
                bool({b"no-writes", "no-writes"} & details["flags"]),
            )
        return self

    def __await__(self: LibraryT) -> Generator[Any, None, LibraryT]:
        return self.initialize().__await__()

    def __getitem__(self, function: str) -> Function[AnyStr] | None:
        return cast(Function[AnyStr] | None, self._functions.get(function))


@overload
def wraps(
    *,
    function_name: str | None = None,
    runtime_checks: bool = ...,
    readonly: bool = ...,
    verify_existence: bool = ...,
) -> Callable[[Callable[P, CommandRequest[R]]], Callable[P, CommandRequest[R]]]: ...


@overload
def wraps(
    *,
    function_name: str | None = None,
    callback: Callable[..., CommandResponseT],
    runtime_checks: bool = ...,
    readonly: bool = ...,
    verify_existence: bool = ...,
) -> Callable[
    [Callable[P, CommandRequest[Any]]], Callable[P, CommandRequest[CommandResponseT]]
]: ...


@versionadded(version="3.5.0")
def wraps(
    *,
    function_name: str | None = None,
    callback: Callable[..., CommandResponseT] = NoopCallback(),
    runtime_checks: bool = False,
    readonly: bool = False,
    verify_existence: bool = True,
) -> Callable[[Callable[P, CommandRequest[Any]]], Callable[P, CommandRequest[Any]]]:
    """
    Decorator for wrapping methods of subclasses of :class:`Library`
    as entry points to the functions contained in the library. This allows
    exposing a strict signature instead of that which :meth:`Function.__call__`
    provides. The callable being decorated should **not** have an implementation as
    it will never be called. The name of the function decorated must match the foreign
    (Lua) function's name.

    The main objective of the decorator is to allow you to represent a lua library of
    functions as a python class having strict (and type safe) methods as entry points.
    Internally the decorator separates ``keys`` from ``args`` before calling
    :meth:`coredis.Redis.fcall`.

    Mapping the decorated method's arguments to key providers is done by type
    annotations: all parameters annotated as `KeyT` will be passed as keys, and the
    rest will be passed as arguments.

    The following example demonstrates most of the functionality provided by the
    decorator::

        from typing import AnyStr

        import coredis
        from coredis.commands import CommandRequest, Library
        from coredis.commands.function import wraps
        from coredis.typing import KeyT, ValueT

        class MyAwesomeLibrary(Library[AnyStr]):
            NAME = "mylib"
            CODE = \"\"\"
            #!lua name=mylib

            redis.register_function('echo', function(k, a)
              return a[1]
            end)
            redis.register_function('ping', function()
              return "PONG"
            end)
            redis.register_function {
              function_name = 'get',
              callback = function(k, a)
                return redis.call("GET", k[1])
              end,
              flags = { 'no-writes' }  -- mark as read-only
            }
            redis.register_function('hmmget', function(k, a)
              local values = {}
              local fields = {}
              local response = {}
              local i = 1
              local j = 1

              while a[i] do
                fields[j] = a[i]
                i = i + 2
                j = j + 1
              end

              for idx, key in ipairs(k) do
                values = redis.call("HMGET", key, unpack(fields))
                for idx, value in ipairs(values) do
                  if not response[idx] and value then
                    response[idx] = value
                  end
                end
              end
              for idx, value in ipairs(fields) do
                if not response[idx] then
                  response[idx] = a[idx*2]
                end
              end
              return response
            end)
            \"\"\"

            @wraps()
            def echo(self, value: ValueT) -> CommandRequest[ValueT]: ... # type: ignore[empty-body]

            @wraps()
            def ping(self) -> CommandRequest[bytes]: ... # type: ignore[empty-body]

            @wraps(readonly=True)
            def get(self, key: KeyT) -> CommandRequest[ValueT]: ...  # type: ignore[empty-body]

            @wraps()
            def hmmget(self, *keys: KeyT, **fields_with_values: int) -> CommandRequest[list[ValueT]]: ... # type: ignore[empty-body]

        async def run() -> None:
            client = coredis.Redis()
            async with client:
                lib = await MyAwesomeLibrary(client, replace=True)
                await client.set("hello", "world")
                # True
                await lib.echo("hello world")
                # b"hello world"
                await lib.ping()
                # b"PONG"
                await lib.get("hello")
                # b"world"

                async with client.pipeline(transaction=False) as pipe:
                    pipe.hset("k1", {"c": 3, "d": 4})
                    pipe.hset("k2", {"a": 1, "b": 2})
                    lib = await MyAwesomeLibrary(pipe)
                    res = lib.hmmget("k1", "k2", a=-1, b=-2, c=-3, d=-4, e=-5)
                print(await res)
                # [b"1", b"2", b"3", b"4", b"-5"]

    :param function_name: Optional name of the registered library function to map this
     entrypoint to. If not provided, the name will be inferred from the method that the
     decorator wraps
    :param callback: a custom callback to execute on the returned value. When provided,
     the callback's type will be inferred as the return type instead of the type from
     the stub.
    :param runtime_checks: Whether to enable runtime type checking of input arguments
     and return values. (requires :pypi:`beartype`). If :data:`False` the function will
     still get runtime type checking if the environment configuration ``COREDIS_RUNTIME_CHECKS``
     is set - for details see :ref:`handbook/typing:runtime type checking`.
    :param readonly: If ``True`` forces this function to use :meth:`coredis.Redis.fcall_ro`
    :param verify_existence: If ``False``, skip check to see if the function has already
     been registered and call FCALL directly. Use this when you want to save a round trip
     and already know the function is registered.

    :return: A function that has a signature mirroring the decorated function.
    """

    def wrapper(
        func: Callable[P, CommandRequest[Any]],
    ) -> Callable[P, CommandRequest[CommandResponseT]]:
        sig = inspect.signature(func)
        first_arg: str = list(sig.parameters.keys())[0]
        runtime_check_wrapper = add_runtime_checks if not runtime_checks else safe_beartype
        key_params = [
            n
            for n, p in sig.parameters.items()
            if p.annotation == "KeyT" or "KeyT" in get_args(p.annotation)
        ]
        arg_fetch: dict[str, Callable[..., Parameters[Any]]] = {
            n: (
                (lambda v: [v])
                if p.kind
                in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.KEYWORD_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }
                else (
                    (lambda v: list(itertools.chain.from_iterable(v.items())))
                    if p.kind == inspect.Parameter.VAR_KEYWORD
                    else lambda v: list(v)
                )
            )
            for n, p in sig.parameters.items()
        }

        def split_args(
            *a: P.args, **k: P.kwargs
        ) -> tuple[Library[AnyStr], Parameters[KeyT], Parameters[ValueT]]:
            bound_arguments = sig.bind(*a, **k)
            bound_arguments.apply_defaults()
            arguments: dict[str, Any] = bound_arguments.arguments
            instance = arguments.pop(first_arg)
            if not isinstance(instance, Library):
                raise RuntimeError(
                    f"{instance.__class__.__name__} is not a subclass of"
                    " coredis.commands.function.Library therefore it's methods cannot be bound "
                    " to a redis library using ``coredis.commands.function.wrap``."
                    " Please refer to the documentation at https://coredis.readthedocs.org/"
                    " for instructions on how to bind a class to a redis library."
                )
            keys: list[KeyT] = []
            args: list[ValueT] = []
            for name in sig.parameters:
                if name == first_arg:
                    continue
                values = arg_fetch[name](arguments[name])
                if name in key_params:
                    keys.extend(values)
                else:
                    args.extend(values)
            return instance, keys, args

        @runtime_check_wrapper
        @functools.wraps(func)
        def _inner(*args: P.args, **kwargs: P.kwargs) -> CommandRequest[CommandResponseT]:
            name = function_name or func.__name__
            instance, keys, arguments = split_args(*args, **kwargs)
            if (fn := instance.functions.get(name, None)) is None:
                if verify_existence:
                    raise AttributeError(
                        f"Library {instance.name} has no registered function {name}"
                    )
                if readonly:
                    return instance.client.fcall_ro(name, keys, arguments).transform(callback)
                return instance.client.fcall(name, keys, arguments).transform(callback)
            return fn(keys, arguments, readonly=readonly).transform(callback)

        return _inner

    return wrapper


class Function(Generic[AnyStr]):
    def __init__(
        self,
        client: coredis.client.Client[AnyStr],
        library_name: StringT,
        name: StringT,
        readonly: bool = False,
    ):
        """
        Wrapper to call a redis function that has already been loaded

        :param library_name: Name of the library under which the function is registered
        :param name: Name of the function this instance represents
        :param readonly: If ``True`` the function will be called with
         :meth:`coredis.Redis.fcall_ro` instead of :meth:`coredis.Redis.fcall`

        Example::

            func = await Function(client, "mylib", "myfunc")
            response = await func(keys=["a"], args=[1])
        """
        self._client: weakref.ReferenceType[coredis.client.Client[AnyStr]] = weakref.ref(client)
        self.library: Library[AnyStr] = Library[AnyStr](client, library_name)
        self.name = name
        self.readonly = readonly

    @property
    def client(self) -> coredis.client.Client[AnyStr]:
        c = self._client()
        assert c
        return c

    def __call__(
        self,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
        callback: Callable[..., CommandResponseT] = NoopCallback(),
        *,
        client: coredis.client.Client[AnyStr] | None = None,
        readonly: bool | None = None,
    ) -> CommandRequest[CommandResponseT]:
        """
        Wrapper to call :meth:`~coredis.Redis.fcall` with the
        function named :paramref:`Function.name` registered under
        the library at :paramref:`Function.library`

        :param keys: The keys this function will reference
        :param args: The arguments expected by the function
        :param callback: a custom callback to call on the raw response from redis before
         returning it.
        :param readonly: If ``True`` forces the function to use :meth:`coredis.Redis.fcall_ro`
        """
        if client is None:
            client = self.client
        if readonly is None:
            readonly = self.readonly

        if readonly:
            return client.fcall_ro(self.name, keys or [], args or []).transform(callback)
        else:
            return client.fcall(self.name, keys or [], args or []).transform(callback)
