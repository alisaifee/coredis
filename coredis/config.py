from __future__ import annotations

import os


class __Config:
    def __init__(self) -> None:
        self.__optimized: bool = False

    @property
    def runtime_checks(self) -> bool:
        """
        Whether runtime type checks are to be enabled.
        Can be enabled by setting the environment variable ``COREDIS_RUNTIME_CHECKS`` to ``true``
        """
        return os.environ.get("COREDIS_RUNTIME_CHECKS", "").lower() in [
            "1",
            "true",
            "t",
        ]

    @property
    def optimized(self) -> bool:
        """
        When ``optimized`` is ``True`` most runtime validations will be disabled.
        This can be enabled in any of the following ways:

          - By running python in optimized mode using the ``-O`` flag
          - By setting the environment variable ``COREDIS_OPTIMIZED`` to ``true``
          - By explicitly setting ``coredis.Config.optimized = True``

        """
        return (
            not __debug__
            or os.environ.get("COREDIS_OPTIMIZED", "").lower()
            in [
                "1",
                "true",
                "t",
            ]
            or self.__optimized
        )

    @optimized.setter
    def optimized(self, value: bool) -> None:
        self.__optimized = value


#: Used to configure global behaviors of the coredis library
Config = __Config()
