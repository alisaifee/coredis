from __future__ import annotations

import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError, DataError
from tests.conftest import server_deprecation_warning, targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_cluster",
    "redis_cluster_raw",
    "keydb",
)
@pytest.mark.asyncio()
class TestGeo:
    async def test_geoadd(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        assert await client.geoadd("barcelona", values) == 2
        assert await client.zcard("barcelona") == 2

    @pytest.mark.min_server_version("6.2.0")
    async def test_geoadd_conditional(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        assert await client.geoadd("barcelona", values) == 2
        assert await client.zcard("barcelona") == 2
        assert await client.geoadd("barcelona", values, change=True) == 0
        assert (
            await client.geoadd(
                "barcelona",
                [
                    (2.2909389952632, 41.533791470673, "place1"),
                    (2.1686, 41.3871, "place3"),
                ],
                condition=PureToken.XX,
                change=True,
            )
            == 1
        )
        assert set(await client.zrange("barcelona", 0, 10)) == {
            _s("place1"),
            _s("place2"),
        }
        assert (
            await client.geoadd(
                "barcelona",
                [
                    (2.2909389952632, 41.533791470673, "place1"),
                    (2.1686, 41.3871, "place3"),
                ],
                condition=PureToken.NX,
            )
            == 1
        )
        assert set(await client.zrange("barcelona", 0, 10)) == {
            _s("place1"),
            _s("place2"),
            _s("place3"),
        }

    async def test_geodist(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        assert await client.geoadd("barcelona", values) == 2
        assert await client.geodist("barcelona", "place1", "place2") == 3067.4157

    async def test_geodist_units(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        assert (
            await client.geodist("barcelona", "place1", "place2", PureToken.KM)
            == 3.0674
        )

    async def test_geohash(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        assert await client.geohash("barcelona", ["place1", "place2"]) == (
            _s("sp3e9yg3kd0"),
            _s("sp3e9cbc3t0"),
        )

    async def test_geopos_no_value(self, client, _s):
        assert await client.geopos("barcelona", ["place1", "place2"]) == (None, None)

    async def test_geopos(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]
        await client.geoadd("barcelona", values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        locations = await client.geopos("barcelona", ["place1", "place2"])
        assert locations == (
            (2.19093829393386841, 41.43379028184083523),
            (2.18737632036209106, 41.40634178640635099),
        )
        assert locations[0].longitude == 2.1909382939338684
        assert locations[0].latitude == 41.4337902818408352

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.nocluster
    async def test_geosearch(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (2.1873744593677, 41.406342043777, "上海市"),
            (2.583333, 41.316667, "place3"),
        ]
        await client.geoadd("barcelona", values)
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            circle_unit=PureToken.M,
        ) == (_s("place1"),)
        assert await client.geosearch(
            "barcelona",
            longitude=2.187,
            latitude=41.406,
            radius=1000,
            circle_unit=PureToken.M,
        ) == (_s("上海市"),)
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            height=1000,
            width=1000,
            box_unit=PureToken.M,
        ) == (_s("place1"),)
        assert await client.geosearch(
            "barcelona", member="place3", radius=100, circle_unit=PureToken.KM
        ) == (
            _s("上海市"),
            _s("place1"),
            _s("place3"),
        )
        # test count
        assert await client.geosearch(
            "barcelona", member="place3", radius=100, circle_unit=PureToken.KM, count=2
        ) == (_s("place3"), _s("上海市"))
        assert (
            await client.geosearch(
                "barcelona",
                member="place3",
                radius=100,
                circle_unit=PureToken.KM,
                count=1,
                any_=True,
            )
        )[0] in (_s("place1"), _s("place3"), _s("上海市"))

    @pytest.mark.min_server_version("6.2.0")
    async def test_geosearch_member(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "上海市",
            ),
        ]

        await client.geoadd("barcelona", values)
        assert await client.geosearch(
            "barcelona", member="place1", radius=4000, circle_unit=PureToken.M
        ) == (
            _s("上海市"),
            _s("place1"),
        )
        assert await client.geosearch(
            "barcelona", member="place1", radius=10, circle_unit=PureToken.M
        ) == (_s("place1"),)

        assert await client.geosearch(
            "barcelona",
            member="place1",
            radius=4000,
            circle_unit=PureToken.M,
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == (
            (
                _s("上海市"),
                3067.4157,
                3471609625421029,
                (2.187376320362091, 41.40634178640635),
            ),
            (
                _s("place1"),
                0.0,
                3471609698139488,
                (2.1909382939338684, 41.433790281840835),
            ),
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_geosearch_sort(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]
        await client.geoadd("barcelona", values)
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=3000,
            circle_unit=PureToken.M,
            order=PureToken.ASC,
        ) == (_s("place1"), _s("place2"))
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=3000,
            circle_unit=PureToken.M,
            order=PureToken.DESC,
        ) == (_s("place2"), _s("place1"))

    @pytest.mark.min_server_version("6.2.0")
    async def test_geosearch_with(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]
        await client.geoadd("barcelona", values)

        # test a bunch of combinations to test the parse response
        # function.
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            circle_unit=PureToken.KM,
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == (
            (
                _s("place1"),
                0.0881,
                3471609698139488,
                (2.19093829393386841, 41.43379028184083523),
            ),
        )
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            circle_unit=PureToken.KM,
            withdist=True,
            withcoord=True,
        ) == (
            (_s("place1"), 0.0881, None, (2.19093829393386841, 41.43379028184083523)),
        )
        assert await client.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            circle_unit=PureToken.KM,
            withhash=True,
            withcoord=True,
        ) == (
            (
                _s("place1"),
                None,
                3471609698139488,
                (2.19093829393386841, 41.43379028184083523),
            ),
        )
        # test no values.
        assert (
            await client.geosearch(
                "barcelona",
                longitude=2,
                latitude=1,
                radius=1,
                circle_unit=PureToken.KM,
                withdist=True,
                withcoord=True,
                withhash=True,
            )
            == ()
        )

    @pytest.mark.min_server_version("6.2.0")
    async def test_geosearch_negative(self, client, _s):
        # not specifying member nor longitude and latitude
        with pytest.raises(DataError):
            await client.geosearch("barcelona")
        # specifying member and longitude and latitude
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", member="Paris", longitude=2, latitude=1)
        # specifying one of longitude and latitude
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", longitude=2)
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", latitude=2)

        # not specifying radius nor width and height
        with pytest.raises(DataError):
            await client.geosearch("barcelona", member="Paris")
        # specifying radius and width and height
        with pytest.raises(CommandSyntaxError):
            await client.geosearch(
                "barcelona", member="Paris", radius=3, width=2, height=1
            )
        with pytest.raises(CommandSyntaxError):
            await client.geosearch(
                "barcelona", member="Paris", radius=3, width=2, height=1
            )
        # specifying one of width and height
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", member="Paris", width=2)
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", member="Paris", height=2)
        # missing units
        with pytest.raises(CommandSyntaxError):
            await client.geosearch(
                "barcelona",
                member="place1",
                radius=10,
            ) == ("place1",)
        with pytest.raises(CommandSyntaxError):
            await client.geosearch("barcelona", member="place1", width=2, height=1)

        # use any without count
        with pytest.raises(CommandSyntaxError):
            await client.geosearch(
                "barcelona",
                member="place3",
                radius=100,
                circle_unit=PureToken.M,
                any_=True,
            )

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.nocluster
    async def test_geosearchstore(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]
        await client.geoadd("barcelona", values)
        await client.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            circle_unit=PureToken.M,
        )
        assert await client.zrange("places_barcelona", 0, -1) == (_s("place1"),)

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.nocluster
    async def test_geosearchstoredist(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        await client.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            circle_unit=PureToken.M,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert await client.zscore("places_barcelona", "place1") == 88.05060698409301

    async def test_georadius(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)

        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 1000, unit=PureToken.M
            ) == (_s("place1"),)

    async def test_georadius_no_values(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert (
                await client.georadius("barcelona", 1, 2, 1000, unit=PureToken.M) == ()
            )

    async def test_georadius_units(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 1, unit=PureToken.KM
            ) == (_s("place1"),)

    async def test_georadius_with(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)

        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            # test a bunch of combinations to test the parse response
            # function.
            assert await client.georadius(
                "barcelona",
                2.191,
                41.433,
                1,
                unit=PureToken.KM,
                withdist=True,
                withcoord=True,
                withhash=True,
            ) == (
                (
                    _s("place1"),
                    0.0881,
                    3471609698139488,
                    (2.19093829393386841, 41.43379028184083523),
                ),
            )

            assert await client.georadius(
                "barcelona",
                2.191,
                41.433,
                1,
                unit=PureToken.KM,
                withdist=True,
                withcoord=True,
            ) == (
                (
                    _s("place1"),
                    0.0881,
                    None,
                    (2.19093829393386841, 41.43379028184083523),
                ),
            )

            assert await client.georadius(
                "barcelona",
                2.191,
                41.433,
                1,
                unit=PureToken.KM,
                withhash=True,
                withcoord=True,
            ) == (
                (
                    _s("place1"),
                    None,
                    3471609698139488,
                    (2.19093829393386841, 41.43379028184083523),
                ),
            )

            # test no values.
            assert (
                await client.georadius(
                    "barcelona",
                    2,
                    1,
                    1,
                    unit=PureToken.KM,
                    withdist=True,
                    withcoord=True,
                    withhash=True,
                )
                == ()
            )

            with pytest.raises(CommandSyntaxError):
                await client.georadius(
                    "barcelona",
                    2,
                    1,
                    1,
                    unit=PureToken.KM,
                    withdist=True,
                    store="somehere",
                )
            with pytest.raises(CommandSyntaxError):
                await client.georadius(
                    "barcelona",
                    2,
                    1,
                    1,
                    unit=PureToken.KM,
                    withdist=True,
                    storedist="somehere",
                )

    async def test_georadius_count(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 3000, count=1, unit=PureToken.M
            ) == (_s("place1"),)

    @pytest.mark.min_server_version("6.2.0")
    async def test_georadius_count_any(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 3000, count=1, unit=PureToken.M
            ) == (_s("place1"),)

            assert (
                len(
                    await client.georadius(
                        "barcelona",
                        2.191,
                        41.433,
                        3000,
                        count=1,
                        any_=True,
                        unit=PureToken.M,
                    )
                )
                == 1
            )
            with pytest.raises(CommandSyntaxError):
                assert await client.georadius(
                    "barcelona", 2.191, 41.433, 3000, any_=True, unit=PureToken.M
                ) == (_s("place1"),)

    async def test_georadius_sort(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 3000, unit=PureToken.M, order=PureToken.ASC
            ) == (
                _s("place1"),
                _s("place2"),
            )
            assert await client.georadius(
                "barcelona", 2.191, 41.433, 3000, unit=PureToken.M, order=PureToken.DESC
            ) == (
                _s("place2"),
                _s("place1"),
            )

    @pytest.mark.nocluster
    async def test_georadius_store(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            await client.georadius(
                "barcelona",
                2.191,
                41.433,
                1000,
                store="places_barcelona",
                unit=PureToken.M,
            )
        assert await client.zrange("places_barcelona", 0, -1) == (_s("place1"),)

    @pytest.mark.nocluster
    async def test_georadius_storedist(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            await client.georadius(
                "barcelona",
                2.191,
                41.433,
                1000,
                storedist="places_barcelona",
                unit=PureToken.M,
            )
        # instead of save the geo score, the distance is saved.
        assert await client.zscore("places_barcelona", "place1") == 88.05060698409301

    async def test_georadiusmember(self, client, _s):
        values = [
            (2.1909389952632, 41.433791470673, "place1"),
            (
                2.1873744593677,
                41.406342043777,
                "place2",
            ),
        ]

        await client.geoadd("barcelona", values)
        with server_deprecation_warning("Use :meth:`geosearch`", client, "6.2"):
            assert await client.georadiusbymember(
                "barcelona", "place1", 4000, unit=PureToken.M
            ) == (
                _s("place2"),
                _s("place1"),
            )
            assert await client.georadiusbymember(
                "barcelona", "place1", 10, unit=PureToken.M
            ) == (_s("place1"),)

            assert await client.georadiusbymember(
                "barcelona",
                "place1",
                4000,
                withdist=True,
                withcoord=True,
                withhash=True,
                unit=PureToken.M,
            ) == (
                (
                    _s("place2"),
                    3067.4157,
                    3471609625421029,
                    (2.187376320362091, 41.40634178640635),
                ),
                (
                    _s("place1"),
                    0.0,
                    3471609698139488,
                    (2.1909382939338684, 41.433790281840835),
                ),
            )
