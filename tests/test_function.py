import pytest
from redis.exceptions import ResponseError

from .conftest import assert_resp_response, skip_if_server_version_lt

engine = "lua"
lib = "mylib"
lib2 = "mylib2"
function = "redis.register_function{function_name='myfunc', callback=function(keys, \
            args) return args[1] end, flags={ 'no-writes' }}"
function2 = "redis.register_function('hello', function() return 'Hello World' end)"
set_function = "redis.register_function('set', function(keys, args) return \
                redis.call('SET', keys[1], args[1]) end)"
get_function = "redis.register_function('get', function(keys, args) return \
                redis.call('GET', keys[1]) end)"


@skip_if_server_version_lt("7.0.0")
class TestFunction:
    @pytest.fixture(autouse=True)
    def reset_functions(self, r):
        r.function_flush()

    @pytest.mark.onlynoncluster
    def test_function_load(self, r):
        assert b"mylib" == r.function_load(f"#!{engine} name={lib} \n {function}")
        assert b"mylib" == r.function_load(
            f"#!{engine} name={lib} \n {function}", replace=True
        )
        with pytest.raises(ResponseError):
            r.function_load(f"#!{engine} name={lib} \n {function}")
        with pytest.raises(ResponseError):
            r.function_load(f"#!{engine} name={lib2} \n {function}")

    def test_function_delete(self, r):
        r.function_load(f"#!{engine} name={lib} \n {set_function}")
        with pytest.raises(ResponseError):
            r.function_load(f"#!{engine} name={lib} \n {set_function}")
        assert r.fcall("set", 1, "foo", "bar") == b"OK"
        assert r.function_delete("mylib")
        with pytest.raises(ResponseError):
            r.fcall("set", 1, "foo", "bar")

    def test_function_flush(self, r):
        r.function_load(f"#!{engine} name={lib} \n {function}")
        assert r.fcall("myfunc", 0, "hello") == b"hello"
        assert r.function_flush()
        with pytest.raises(ResponseError):
            r.fcall("myfunc", 0, "hello")
        with pytest.raises(ResponseError):
            r.function_flush("ABC")

    @pytest.mark.onlynoncluster
    def test_function_list(self, r):
        r.function_load(f"#!{engine} name={lib} \n {function}")
        res = [
            [
                b"library_name",
                b"mylib",
                b"engine",
                b"LUA",
                b"functions",
                [[b"name", b"myfunc", b"description", None, b"flags", [b"no-writes"]]],
            ]
        ]
        resp3_res = [
            {
                b"library_name": b"mylib",
                b"engine": b"LUA",
                b"functions": [
                    {b"name": b"myfunc", b"description": None, b"flags": [b"no-writes"]}
                ],
            }
        ]
        assert_resp_response(r, r.function_list(), res, resp3_res)
        assert_resp_response(r, r.function_list(library="*lib"), res, resp3_res)
        res[0].extend(
            [b"library_code", f"#!{engine} name={lib} \n {function}".encode()]
        )
        resp3_res[0][b"library_code"] = f"#!{engine} name={lib} \n {function}".encode()
        assert_resp_response(r, r.function_list(withcode=True), res, resp3_res)

    @pytest.mark.onlycluster
    def test_function_list_on_cluster(self, r):
        r.function_load(f"#!{engine} name={lib} \n {function}")
        function_list = [
            [
                b"library_name",
                b"mylib",
                b"engine",
                b"LUA",
                b"functions",
                [[b"name", b"myfunc", b"description", None, b"flags", [b"no-writes"]]],
            ]
        ]
        resp3_function_list = [
            {
                b"library_name": b"mylib",
                b"engine": b"LUA",
                b"functions": [
                    {b"name": b"myfunc", b"description": None, b"flags": [b"no-writes"]}
                ],
            }
        ]
        primaries = r.get_primaries()
        res = {}
        resp3_res = {}
        for node in primaries:
            res[node.name] = function_list
            resp3_res[node.name] = resp3_function_list
        assert_resp_response(r, r.function_list(), res, resp3_res)
        assert_resp_response(r, r.function_list(library="*lib"), res, resp3_res)
        node = primaries[0].name
        code = f"#!{engine} name={lib} \n {function}".encode()
        res[node][0].extend([b"library_code", code])
        resp3_res[node][0][b"library_code"] = code
        assert_resp_response(r, r.function_list(withcode=True), res, resp3_res)

    @pytest.mark.onlynoncluster
    def test_function_stats(self, r):
        r.function_load(f"#!{engine} name={lib} \n {function}")
        res = [
            b"running_script",
            None,
            b"engines",
            [b"LUA", [b"libraries_count", 1, b"functions_count", 1]],
        ]
        resp3_res = {
            b"running_script": None,
            b"engines": {b"LUA": {b"libraries_count": 1, b"functions_count": 1}},
        }
        assert_resp_response(r, r.function_stats(), res, resp3_res)

    def test_fcall(self, r):
        r.function_load(f"#!{engine} name={lib} \n {set_function}")
        r.function_load(f"#!{engine} name={lib2} \n {get_function}")
        assert r.fcall("set", 1, "foo", "bar") == b"OK"
        assert r.fcall("get", 1, "foo") == b"bar"
        with pytest.raises(ResponseError):
            r.fcall("myfunc", 0, "hello")

    def test_fcall_ro(self, r):
        r.function_load(f"#!{engine} name={lib} \n {function}")
        assert r.fcall_ro("myfunc", 0, "hello") == b"hello"
        r.function_load(f"#!{engine} name={lib2} \n {set_function}")
        with pytest.raises(ResponseError):
            r.fcall_ro("set", 1, "foo", "bar")

    def test_function_dump_restore(self, r):
        r.function_load(f"#!{engine} name={lib} \n {set_function}")
        payload = r.function_dump()
        assert r.fcall("set", 1, "foo", "bar") == b"OK"
        r.function_delete("mylib")
        with pytest.raises(ResponseError):
            r.fcall("set", 1, "foo", "bar")
        assert r.function_restore(payload)
        assert r.fcall("set", 1, "foo", "bar") == b"OK"
        r.function_load(f"#!{engine} name={lib2} \n {get_function}")
        assert r.fcall("get", 1, "foo") == b"bar"
        r.function_delete("mylib")
        assert r.function_restore(payload, "FLUSH")
        with pytest.raises(ResponseError):
            r.fcall("get", 1, "foo")
