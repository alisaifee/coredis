from __future__ import annotations

import functools
import hashlib
import inspect
import itertools
from typing import TYPE_CHECKING, Any, cast, get_args, overload

from deprecated.sphinx import versionadded

from coredis._utils import b
from coredis.commands import CommandRequest, CommandResponseT
from coredis.exceptions import NoScriptError
from coredis.response._callbacks import NoopCallback
from coredis.retry import ConstantRetryPolicy
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    Generic,
    KeyT,
    P,
    Parameters,
    R,
    ResponseType,
    StringT,
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
        registered_client: coredis.client.Client[AnyStr] | None = None,
        script: StringT | None = None,
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
        self.registered_client: coredis.client.Client[AnyStr] | None = registered_client
        self.script: StringT
        if not script:
            raise RuntimeError("No script provided")
        self.script = script
        self.sha = hashlib.sha1(b(script)).hexdigest()  # type: ignore
        self.readonly = readonly

    def __call__(
        self,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
        client: coredis.client.Client[AnyStr] | None = None,
        readonly: bool | None = None,
        callback: Callable[..., CommandResponseT] = NoopCallback(),
    ) -> CommandRequest[CommandResponseT]:
        """
        Executes the script registered in :paramref:`Script.script` using
        :meth:`coredis.Redis.evalsha`. Additionally, if the script was not yet
        registered on the instance, it will automatically do that as well
        and cache the sha at :data:`Script.sha`

        :param keys: The keys this script will reference
        :param args: The arguments expected by the script
        :param client: The redis client to use instead of :paramref:`Script.client`
        :param readonly: If ``True`` forces the script to be called with
         :meth:`coredis.Redis.evalsha_ro`
        :param callback: a custom callback to call on the raw response from redis before
         returning it.
        """
        from coredis.pipeline import ClusterPipeline, Pipeline

        if client is None:
            client = self.registered_client
        if not client:
            raise RuntimeError(
                "This instance is not bound to a redis client."
                "Please provide a valid instance to execute the script with"
            )
        if readonly is None:
            readonly = self.readonly

        method = client.evalsha_ro if readonly else client.evalsha

        if isinstance(client, (ClusterPipeline, Pipeline)):
            cast(Pipeline[AnyStr], client).scripts.add(self)
            return method(self.sha, keys=keys, args=args).transform(callback)
        else:
            return (
                method(self.sha, keys=keys, args=args)
                .retry(
                    ConstantRetryPolicy((NoScriptError,), retries=1, delay=0),
                    lambda _: client.script_load(self.script),
                )
                .transform(callback)
            )

    async def execute(
        self,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
        client: coredis.client.Client[AnyStr] | None = None,
        readonly: bool | None = None,
    ) -> ResponseType:
        """
        Executes the script registered in :paramref:`Script.script`

        :meta private:
        """
        return await self(keys, args, client, readonly)

    @overload
    def wraps(
        self,
        *,
        client_arg: str | None = ...,
        runtime_checks: bool = ...,
        readonly: bool = ...,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...

    @overload
    def wraps(
        self,
        *,
        callback: Callable[..., CommandResponseT],
        client_arg: str | None = ...,
        runtime_checks: bool = ...,
        readonly: bool = ...,
    ) -> Callable[[Callable[P, Awaitable[Any]]], Callable[P, Awaitable[CommandResponseT]]]: ...

    @versionadded(version="3.5.0")
    def wraps(
        self,
        *,
        callback: Callable[..., CommandResponseT] = NoopCallback(),
        client_arg: str | None = None,
        runtime_checks: bool = False,
        readonly: bool = False,
    ) -> Any:
        """
        Decorator for wrapping a regular python function, method or classmethod
        signature with a :class:`~coredis.commands.script.Script`. This allows
        exposing a strict signature instead of that which :meth:`__call__` provides.
        The callable being decorated should **not** have an implementation as
        it will never be called.

        The main objective of the decorator is to allow you to have strict (and type safe)
        signatures for wrappers for lua scripts. Internally the decorator separates
        ``keys`` from ``args`` before calling :meth:`coredis.Redis.evalsha`.

        Mapping the decorated method's arguments to key providers is done by type
        annotations: all parameters annotated as `KeyT` will be passed as keys, and the
        rest will be passed as arguments.

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
            from coredis.commands import Command Request

            client = coredis.Redis()
            @client.register_script("return {KEYS[1], ARGV[1]}").wraps()
            def echo_key_value(key: KeyT, value: ValueT) -> CommandRequest[list[ValueT]]: ...

            async with client:
                res = await echo_key_value("co", "redis")
            # [b"co", b"redis"]

        Alternatively, the following example builds a class method that requires
        the ``client`` to be passed in explicitly::

            from coredis import Redis
            from coredis.commands import Script
            from coredis.typing import StringT

            class ScriptProvider:
                @classmethod
                @Script(script="return KEYS[1]").wraps(
                    client_arg="client"
                )
                def echo_key(cls, client, key: KeyT) -> CommandRequest[StringT]: ...

                @classmethod
                @Script(script="return ARGS[1]").wraps(
                    client_arg="client"
                )
                def echo_arg(cls, client, value) -> CommandRequest[StringT]: ...

            async with Redis() as client:
                echoed = await ScriptProvider.echo_key(client, "coredis")
                # b"coredis"
                echoed = await ScriptProvider.echo_value(client, "coredis")
                # b"coredis"

        You can also pass a custom callback to execute on the return type, which will
        be inferred as the return type rather than the annotation::

            class MyCallback(ResponseCallback[Any, Any, int]):
                def transform(self, response: ResponseType) -> int:
                    return sum([ord(c) for c in str(response)])

            client = coredis.Redis(decode_responses=True)
            async with client:
                script = client.register_script("return {KEYS[1], ARGV[1]}")

                # we use Any since return type will come from callback
                @script.wraps(callback=MyCallback())
                def echo_key_value(key: KeyT, value: ValueT) -> CommandRequest[int]: ...

                res = await echo_key_value("co", "redis")
                reveal_type(res)  # int
                # 1161

        :param callback: a custom callback to execute on the returned value. When provided,
         the callback's type will be inferred as the return type instead of the type from
         the stub.
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

        def wrapper(func: Callable[P, Awaitable[Any]]) -> Callable[P, Awaitable[CommandResponseT]]:
            sig = inspect.signature(func)
            first_arg = None
            if args := list(sig.parameters.keys()):
                first_arg = args[0]
            runtime_check_wrapper = add_runtime_checks if not runtime_checks else safe_beartype
            script_instance = self
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
            if first_arg in {"self", "cls"}:
                arg_fetch.pop(first_arg)
            if client_arg:
                arg_fetch.pop(client_arg)

            def split_args(
                bound_arguments: inspect.BoundArguments,
            ) -> tuple[
                Parameters[KeyT],
                Parameters[ValueT],
                coredis.client.Client[AnyStr] | None,
            ]:
                bound_arguments.apply_defaults()
                arguments = bound_arguments.arguments
                keys: list[KeyT] = []
                args: list[ValueT] = []
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
            def __inner(
                *args: P.args,
                **kwargs: P.kwargs,
            ) -> CommandRequest[CommandResponseT]:
                keys, arguments, client = split_args(sig.bind(*args, **kwargs))
                return script_instance(keys, arguments, client, readonly, callback=callback)

            return __inner

        return wrapper
