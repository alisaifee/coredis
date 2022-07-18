from __future__ import annotations

import functools
import inspect
import itertools
import weakref
from typing import Any, ClassVar, cast

from deprecated.sphinx import versionadded

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.exceptions import FunctionError
from coredis.typing import (
    TYPE_CHECKING,
    AnyStr,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Generic,
    KeyT,
    List,
    Optional,
    P,
    Parameters,
    R,
    ResponseType,
    StringT,
    Tuple,
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
    NAME: ClassVar[Optional[StringT]] = None
    #: Class variable equivalent of the :paramref:`Library.code` argument.
    CODE: ClassVar[Optional[StringT]] = None

    def __init__(
        self,
        client: coredis.client.Client[AnyStr],
        name: Optional[StringT] = None,
        code: Optional[StringT] = None,
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
        self._client: weakref.ReferenceType[
            coredis.client.Client[AnyStr]
        ] = weakref.ref(client)
        self.name = nativestr(name or self.NAME)
        self.code = (code or self.CODE or "").lstrip()
        self._functions: EncodingInsensitiveDict = EncodingInsensitiveDict()
        self.replace = replace
        if self.replace and not self.code:
            raise RuntimeError(
                "library code must be provided when the ``replace`` option is used"
            )

    @property
    def client(self) -> coredis.client.Client[AnyStr]:
        c = self._client()
        assert c
        return c

    @property
    def functions(self) -> Dict[str, Function[AnyStr]]:
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
        self._functions.clear()
        library = (await self.client.function_list(self.name)).get(self.name)
        if (not library and self.code) or (replace or self.replace):
            await self.client.function_load(self.code, replace=replace or self.replace)
            library = (await self.client.function_list(self.name)).get(self.name)

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

    def __getitem__(self, function: str) -> Optional[Function[AnyStr]]:
        return cast(Optional[Function[AnyStr]], self._functions.get(function))

    @classmethod
    @versionadded(version="3.5.0")
    def wraps(
        cls,
        function_name: str,
        key_spec: Optional[List[KeyT]] = None,
        param_is_key: Callable[[inspect.Parameter], bool] = lambda p: (
            p.annotation in {"KeyT", KeyT}
        ),
        runtime_checks: bool = False,
        readonly: Optional[bool] = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        """
        Decorator for wrapping methods of subclasses of :class:`Library`
        as entry points to the functions contained in the library. This allows
        exposing a strict signature instead of that which :meth:`Function.__call__`
        provides. The callable being decorated should **not** have an implementation as
        it will never be called.

        The main objective of the decorator is to allow you to represent a lua library of
        functions as a python class having strict (and type safe) methods as entry points.
        Internally the decorator separates ``keys`` from ``args`` before calling
        :meth:`coredis.Redis.fcall`.

        Mapping the decorated method's arguments to key providers is done either by
        using :paramref:`key_spec` or :paramref:`param_is_key`. All other parameters of the
        decorated method are assumed to be ``args`` consumed by the lua function.


        The following example demonstrates most of the functionality provided by the
        decorator::

            import coredis
            from coredis.commands import Library
            from coredis.typing import KeyT, ValueT
            from typing import List

            class MyAwesomeLibrary(Library):
                NAME = "mylib"
                CODE = \"\"\"
                #!lua name=mylib

                redis.register_function('echo', function(k, a)
                    return a[1]
                end)
                redis.register_function('ping', function()
                    return "PONG"
                end)
                redis.register_function('get', function(k, a)
                    return redis.call("GET", k[1])
                end)
                redis.register_function('hmget', function(k, a)
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

                @Library.wraps("echo")
                async def echo(self, value: ValueT) -> ValueT: ...

                @Library.wraps("ping")
                async def ping(self) -> str: ...

                @Library.wraps("get")
                async def get(self, key: KeyT) -> ValueT: ...

                @Library.wraps("hmmget")
                async def hmmget(self, *keys: KeyT, **fields_with_values: ValueT): ...
                \"\"\"
                Return values of ``fields_with_values`` on a first come first serve
                basis from the hashes at ``keys``. Since ``fields_with_values`` is a mapping
                the keys are mapped to hash fields and the values are used
                as defaults if they are not found in any of the hashes at ``keys``
                \"\"\"

            client = coredis.Redis()
            lib = await MyAwesomeLibrary(client, replace=True)
            await client.set("hello", "world")
            # True
            await lib.echo("hello world")
            # b"hello world"
            await lib.ping()
            # b"pong"
            await lib.get("hello")
            # b"hello"
            await client.hset("k1", {"c": 3, "d": 4})
            await client.hset("k2", {"a": 1, "b": 2})
            await lib.hmmget("k1", "k2", a=-1, b=-2, c=-3, d=-4, e=-5)
            # [b"1", b"2", b"3", b"4", b"-5"]

        :param key_spec: list of parameters of the decorated method that will
         be passed as the :paramref:`keys` argument to :meth:`__call__`. If provided
         this parameter takes precedence over using :paramref:`param_is_key` to
         determine if a parameter is a key provider.
        :param param_is_key: a callable that accepts a single argument of type
         :class:`inspect.Parameter` and returns ``True`` if the parameter points to a key
         that should be appended to the :paramref:`__call__.keys` argument of
         :meth:`__call__`. The default implementation marks a parameter as a key
         provider if it is of type :data:`coredis.typing.KeyT` and is only used
         if :paramref:`key_spec` is ``None``.
        :param runtime_checks: Whether to enable runtime type checking of input arguments
         and return values. (requires :pypi:`beartype`). If :data:`False` the function will
         still get runtime type checking if the environment configuration ``COREDIS_RUNTIME_CHECKS``
         is set - for details see :ref:`handbook/typing:runtime type checking`.
        :param readonly: If ``True`` forces this function to use :meth:`coredis.Redis.fcall_ro`

        :return: A function that has a signature mirroring the decorated function.
        """

        def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            sig = inspect.signature(func)
            first_arg: str = list(sig.parameters.keys())[0]
            runtime_check_wrapper = (
                add_runtime_checks if not runtime_checks else safe_beartype
            )
            key_params = (
                key_spec
                if key_spec
                else [n for n, p in sig.parameters.items() if param_is_key(p)]
            )
            arg_fetch: Dict[str, Callable[..., Parameters[Any]]] = {
                n: (lambda v: [v])
                if p.kind
                in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.KEYWORD_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }
                else (lambda v: list(itertools.chain.from_iterable(v.items())))
                if p.kind == inspect.Parameter.VAR_KEYWORD
                else lambda v: list(v)
                for n, p in sig.parameters.items()
            }

            def split_args(
                *a: P.args, **k: P.kwargs
            ) -> Tuple[Library[AnyStr], Parameters[KeyT], Parameters[ValueT]]:
                bound_arguments = sig.bind(*a, **k)
                bound_arguments.apply_defaults()
                arguments: Dict[str, Any] = bound_arguments.arguments
                instance: Library[AnyStr]
                if not isinstance(instance := arguments.pop(first_arg), Library):
                    raise RuntimeError(
                        f"{instance.__class__.__name__} is not a subclass of"
                        " coredis.commands.function.Library therefore it's methods cannot be bound "
                        " to a redis library using ``Library.wrap``."
                        " Please refer to the documentation at https://coredis.readthedocs.org/"
                        " for instructions on how to bind a class to a redis library."
                    )
                keys: List[KeyT] = []
                args: List[ValueT] = []
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
            async def _inner(*args: P.args, **kwargs: P.kwargs) -> R:
                instance, keys, arguments = split_args(*args, **kwargs)
                func = instance.functions[function_name]
                if not func:
                    raise AttributeError(
                        f"Library {instance.name} has no registered function {function_name}"
                    )
                # TODO: atleast lie with a cast.
                #  mypy doesn't like the cast. pyright is ok with it
                return await func(keys, arguments, readonly=readonly)  # type: ignore

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
        self._client: weakref.ReferenceType[
            coredis.client.Client[AnyStr]
        ] = weakref.ref(client)
        self.library: Library[AnyStr] = Library[AnyStr](client, library_name)
        self.name = name
        self.readonly = readonly

    @property
    def client(self) -> coredis.client.Client[AnyStr]:
        c = self._client()
        assert c
        return c

    async def initialize(self) -> Function[AnyStr]:
        await self.library
        return self

    def __await__(self) -> Generator[Any, None, Function[AnyStr]]:
        return self.initialize().__await__()

    async def __call__(
        self,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
        *,
        client: Optional[coredis.client.Client[AnyStr]] = None,
        readonly: Optional[bool] = None,
    ) -> ResponseType:
        """
        Wrapper to call :meth:`~coredis.Redis.fcall` with the
        function named :paramref:`Function.name` registered under
        the library at :paramref:`Function.library`

        :param keys: The keys this function will reference
        :param args: The arguments expected by the function
        :param readonly: If ``True`` forces the function to use :meth:`coredis.Redis.fcall_ro`
        """
        if client is None:
            client = self.client
        if readonly is None:
            readonly = self.readonly

        if readonly:
            return await client.fcall_ro(self.name, keys or [], args or [])
        else:
            return await client.fcall(self.name, keys or [], args or [])
