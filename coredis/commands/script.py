from __future__ import annotations

import functools
import hashlib
import inspect
import itertools
from typing import TYPE_CHECKING, Any, cast

from deprecated.sphinx import versionadded

from coredis._protocols import SupportsScript
from coredis._utils import b
from coredis.exceptions import NoScriptError
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    Dict,
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
    ValueT,
    add_runtime_checks,
    safe_beartype,
)

if TYPE_CHECKING:
    import coredis.client


class Script(Generic[AnyStr]):
    """
    An executable Lua script object returned by :meth:`coredis.Redis.register_script`.
    Instances of the class are callable and take arguments in the same shape
    as :meth:`coredis.Redis.evalsha` or :meth:`coredis.Redis.eval`
    (i.e a list of :paramref:`~__call__.keys` and :paramref:`~__call__.args`).

    Example::

        client = coredis.Redis()
        await client.set("test", "co")
        concat = client.register_script("return redis.call('GET', KEYS[1]) + ARGV[1]")
        assert await concat(['test'], ['redis']) == "coredis"
    """

    #: SHA of this script once it's registered with the redis server
    sha: AnyStr

    def __init__(
        self,
        registered_client: Optional[SupportsScript[AnyStr]] = None,
        script: Optional[StringT] = None,
        readonly: bool = False,
    ):
        """
        :param client: The client to use for executing the lua script. If
         this is ``None`` the client will have to be provided when invoking
         the script using :meth:`__call__` with the :paramref:`__call__.client`
         parameter.
        :param script: The lua script that will be used by :meth:`__call__`
        :param readonly: If ``True`` the script will be called with
         :meth:`coredis.Redis.evalsha_ro` instead of :meth:`coredis.Redis.evalsha`
        """
        self.registered_client: Optional[SupportsScript[AnyStr]] = registered_client
        self.script: StringT
        if not script:
            raise RuntimeError("No script provided")
        self.script = script
        self.sha = hashlib.sha1(b(script)).hexdigest()  # type: ignore
        self.readonly = readonly

    async def __call__(
        self,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
        client: Optional[SupportsScript[AnyStr]] = None,
        readonly: Optional[bool] = None,
    ) -> ResponseType:
        """
        Executes the script registered in :paramref:`Script.script` using
        :meth:`coredis.Redis.evalsha`. Additionally if the script was not yet
        registered on the instance, it will automatically do that as well
        and cache the sha at :data:`Script.sha`

        :param keys: The keys this script will reference
        :param args: The arguments expected by the script
        :param client: The redis client to use instead of :paramref:`Script.client`
        :param readonly: If ``True`` forces the script to be called with
         :meth:`coredis.Redis.evalsha_ro`
        """
        from coredis.pipeline import Pipeline

        if client is None:
            client = self.registered_client
        if not client:
            raise RuntimeError(
                "This instance is not bound to a redis client."
                "Please provide a valid instance to execute the script with"
            )
        if readonly is None:
            readonly = self.readonly

        # make sure the Redis server knows about the script
        if isinstance(client, Pipeline):
            # make sure this script is good to go on pipeline
            cast(Pipeline[AnyStr], client).scripts.add(self)

        method = client.evalsha_ro if readonly else client.evalsha
        try:
            return cast(ResponseType, await method(self.sha, keys=keys, args=args))
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = cast(AnyStr, await client.script_load(self.script))
            return cast(ResponseType, await method(self.sha, keys=keys, args=args))

    async def execute(
        self,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
        client: Optional[SupportsScript[AnyStr]] = None,
        readonly: Optional[bool] = None,
    ) -> ResponseType:
        """
        Executes the script registered in :paramref:`Script.script`

        :meta private:
        """
        return await self(keys, args, client, readonly)

    @versionadded(version="3.5.0")
    def wraps(
        self,
        key_spec: Optional[List[str]] = None,
        param_is_key: Callable[[inspect.Parameter], bool] = lambda p: (
            p.annotation in {"KeyT", KeyT}
        ),
        client_arg: Optional[str] = None,
        runtime_checks: bool = False,
        readonly: Optional[bool] = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        """
        Decorator for wrapping a regular python function, method or classmethod
        signature with a :class:`~coredis.commands.script.Script`. This allows
        exposing a strict signature instead of that which :meth:`__call__` provides.
        The callable being decorated should **not** have an implementation as
        it will never be called.

        The main objective of the decorator is to allow you to have strict (and type safe)
        signatures for wrappers for lua scripts. Internally the decorator separates
        ``keys`` from ``args`` before calling :meth:`coredis.Redis.evalsha`. Mapping the
        decorated methods arguments to key providers is done either by using :paramref:`key_spec`
        or :paramref:`param_is_key`. All other paramters of the decorated function are assumed
        to be ``args`` consumed by the lua script.

        By default the decorated method is bound to the :class:`coredis.client.Redis`
        or :class:`coredis.client.RedisCluster` instance that the :class:`Script` instance
        was instantiated with. This may however not be the instance you want to eventually
        execute the method with. For such scenarios the decorated method can accept an additional
        parameter which has the name declared by :paramref:`client_arg`).

        The following example executes the script with the ``client`` instance used
        to register the script. The ``key`` parameter is detected as a key provider
        as it is annotated with the :data:`coredis.typing.KeyT` type, and ``value`` is
        passed to redis as an ``arg``::

            import coredis
            from coredis.typing import KeyT, ValueT
            from typing import List

            client = coredis.Redis()
            @client.register_script("return {KEYS[1], ARGV[1]}").wraps()
            async def echo_key_value(key: KeyT, value: ValueT) -> List[ValueT]: ...

            k, v = await echo_key_value("co", "redis")
            # (b"co", b"redis")

        Alternatively, the following example builds a class method that requires
        the ``client`` to be passed in explicitly::

            from coredis import Redis
            from coredis.commands import Script

            class ScriptProvider:
                @classmethod
                @Script(script="return KEYS[1]").wraps(
                    key_spec=["key"],
                    client_arg="client"
                )
                def echo_key(cls, client, key): ...

                @classmethod
                @Script(script="return ARGS[1]").wraps(
                    client_arg="client"
                )
                def echo_arg(cls, client, value): ...

            echoed = await ScriptProvider.echo_key(Redis(), "coredis")
            # b"coredis"
            echoed = await ScriptProvider.echo_value(Redis(), "coredis")
            # b"coredis"

        :param key_spec: list of parameters of the decorated method that will
         be passed as the :paramref:`keys` argument to :meth:`__call__`. If provided
         this parameter takes precedence over using :paramref:`param_is_key` to determine if
         a parameter is a key provider.
        :param param_is_key: a callable that accepts a single argument of type
         :class:`inspect.Parameter` and returns ``True`` if the parameter points
         to a key that should be appended to the :paramref:`__call__.keys` argument
         of :meth:`__call__`. The default implementation marks a parameter as a key
         provider if it is of type :data:`coredis.typing.KeyT` and is only used if
         :paramref:`key_spec` is ``None``.
        :param client_arg: The parameter of the decorator that will contain a client instance
         to be used to execute the script.
        :param runtime_checks: Whether to enable runtime type checking of input arguments
         and return values. (requires :pypi:`beartype`). If :data:`False` the function will
         still get runtime type checking if the environment configuration ``COREDIS_RUNTIME_CHECKS``
         is set - for details see :ref:`handbook/typing:runtime type checking`.
        :param readonly: If ``True`` forces this script to be called with
         :meth:`coredis.Redis.evalsha_ro`

        :return: A function that has a signature mirroring the decorated function.
        """

        def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            sig = inspect.signature(func)
            first_arg = list(sig.parameters.keys())[0]
            runtime_check_wrapper = (
                add_runtime_checks if not runtime_checks else safe_beartype
            )
            script_instance = self
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
            if first_arg in {"self", "cls"}:
                arg_fetch.pop(first_arg)
            if client_arg:
                arg_fetch.pop(client_arg)

            def split_args(
                bound_arguments: inspect.BoundArguments,
            ) -> Tuple[
                Parameters[KeyT],
                Parameters[ValueT],
                Optional[coredis.client.Client[AnyStr]],
            ]:
                bound_arguments.apply_defaults()
                arguments = bound_arguments.arguments
                keys: List[KeyT] = []
                args: List[ValueT] = []
                for name in sig.parameters:
                    if name not in arg_fetch:
                        continue
                    value = arg_fetch[name](arguments[name])
                    if name in key_params:
                        keys.extend(value)
                    else:
                        args.extend(value)
                return (
                    keys,
                    args,
                    arguments.get(client_arg) if client_arg else None,
                )

            @runtime_check_wrapper
            @functools.wraps(func)
            async def __inner(
                *args: P.args,
                **kwargs: P.kwargs,
            ) -> R:
                keys, arguments, client = split_args(sig.bind(*args, **kwargs))
                # TODO: atleast lie with a cast.
                #  mypy doesn't like the cast. pyright is ok with it
                return await script_instance(keys, arguments, client, readonly)  # type: ignore

            return __inner

        return wrapper
