Development
-----------

Testing
^^^^^^^

Since the tests require various configurations and versions of redis,
a docker-compose file is provided to make running tests easier.

This requires a working `docker & docker-compose installation <https://docs.docker.com/compose/gettingstarted/>`_.

The unit tests will lazily initialize the containers required per test using the
`lovely-pytest-docker <https://github.com/lovelysystems/lovely-pytest-docker>`_  plugin.

.. code-block:: bash

    $ uv run pytest tests


To reduce unnecessary setup and tear down the containers are left running after the tests complete. To clean up:

.. code-block:: bash

    $ docker-compose down --remove-orphans

You can run single tests or filter out certain client types like this:

.. code-block:: bash

    $ pytest -m 'basic and not (raw or resp2 or cached)' tests/commands/test_string.py
