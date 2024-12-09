# Simple Setup Process

This intended as a simplified setup set-by-step guide (in comparison to the [README](../README.md)).

## Decide Paths

You need to decide on destinations for the following:

- A runtime for Riak instances running tests (e.g. "~/rt/riak")

- A destination in which to build riak releases to be tested (e.g. "~/test_build/current/riak" and "~test_build/previous/riak")

- A location for the riak_test software itself, where the tests will be built and test scripts will be run and logs will be stored (e.g. "~/riak_test")

There will also need to be a test configuration file (".riak_test.config"), which must be in the root of the home directory of the user running tests.

## Prerequisites

Erlang must be installed na running on the machine.  A simple way of achieving this is via [kerl](https://github.com/kerl/kerl).  All parts should be built and the tests run using the same Erlang version: riak_test, current riak and previous riak.

## Clone Repos

Clone riak into your build area, and switch to the tag or branch you wish to test i.e.:

```bash
cd ~/test_build/current
git clone https://github.com/basho/riak
cd riak
git checkout riak-3.2.3
```

```bash
cd ~/test_build/previous
git clone https://github.com/basho/riak
cd riak
git checkout riak-3.2.1
```

Clone riak_test into your test software location, checkout the test branch you intend to use, and `make all` i.e.:

```bash
cd ~
git clone https://github.com/basho/riak_test
cd riak_test
git checkout openriak-3.2
make all
```

## Setup initial test Configuration

Create a `~/.riak_test.config` file with the following sample configuration:

```
{default, [

    {rt_max_wait_time, 600000},
    {rt_retry_delay, 1000},
    {rt_harness, rtdev},
    {rt_scratch_dir, "/tmp/riak_test_scratch"},
    {spam_dir, "/Users/sgtwilko/dbtest/OpenRiak/riak_test/search-corpus/spam.0"},
    {platform, "osx-64"},
    {conn_fail_time, 60000},
    {organisation, nhse},
    {deps, ["./_build/default/lib/"]}

]}.

{rtdev, [

    {rt_project, "riak"},
    {rt_default_config, [{riak_kv, [{handoff_deletes, true}]}]},
    {test_paths, ["_build/test/lib/riak_test/tests"]},
    {
        rtdev_path, 
        [
            {root,     "/Users/sgtwilko/rt/riak"},
            {current,  "/Users/sgtwilko/rt/riak/current"},
            {previous, "/Users/sgtwilko/rt/riak/previous"}
        ]
    }
]}.
```

The `spam_dir` will need to point at the location of the riak_test software.  At that location there will be a tarred/zipped file containing test data, which you will need to untar/unzip to populate the spam directory.  This is only relevant to a single test - `partition_repair`.  Some tests may also require a version of OpenSSL to be available within the PATH.

The platform shout be set to `osx-64` or `linux`.

There are issues with using relative references for some paths, so use fully-qualified file paths were possible.

## Setup Test Runtime

Initialise git in the test runtime environment i.e.:

```bash
cd ~/riak_test
./bin/rtdev-setup-releases.sh
```

This script will make `~/rt/riak` the path to setup releases, so make sure this is the same path used in the `.riak_test.config` file.  If this is to be overridden, then it can be do so by override [$RT_DEST_DIR](../bin/rtdev-setup-releases.sh#L11).  The test folders are reset back to default each time using git (and this is what the setup script will initiate).

## Build each Test version

Within the test_build are, it is necessary to build each release and copy them over to the run time environment using the install script.  

Assuming each riak folder is currently set to the correct release or branch:

```bash
cd ~/test_build/current
make devclean; make devrel
~/riak_test/bin/rtdev-install.sh current

cd ~/test_build/previous
make devclean; make devrel
~/riak_test/bin/rtdev-install.sh previous
```

Each time you change a branch for testing, re-run the `make devclean; make devrel` and the `rtdev-install.sh` script to update the current or previous test destination as appropriate.  When changing to a tag, the tag will be made with the correct `rebar.lock` file, however this will not necessarily be the case with a branch.  If looking to test a branch rather than a tag, always remove the `rebar.lock` file from the root of the riak folder before running `make devclean; make devrel`.

The riak `make devrel` process does not make a full copy of the riak installation, using symbolic links instead.  So it is important not to modify these riak folders even after running the install script - as tests may be impacted.

## Run Tests

There are two primary ways of running tests: as a group; or an running a single test in isolation.

### Run a test 'group'

There are pre-defined groups of tests under the `groups` folder in `riak_test`, or you could define your own.  The group is a list of test names, and the test script will run each of these tests in alphabetical order.

```bash
# To run the 'kv_all' group with the bitcask backend
cd ~/riak_test
./group.sh -g kv_all -c rtdev -b bitcask

# To run the '2i_all' group with the leveled backend
cd ~/riak_test
./group.sh -g 2i_all -c rtdev -b leveled
```

The `-c` reference must refer to a stanza in your `~/.riak_test.config`.

Test groups will ignore failures, and continue with other tests in the group.  At the end of all tests, a report of pass/fail by test is printed to screen, and to a log file.

The following test groups and backends are tested as part of a release:

```
kv_all (leveled)
nextgenrepl (leveled)
core_all (leveled)
datatypes_all (leveled)
repl_all (leveled)
2i_all (leveled)
ensemble (leveled)
admin_all (leveled)
mapred_all (leveled)
pipe_all (leveled)
rtc_all (leveled)
kv_all (bitcask)
nextgenrepl (bitcask)
bitcask_only (bitcask)
smoke (eleveldb)
eleveldb_only (eleveldb)
```

Test groups `kv_all`, `nextgenrepl`, `repl_all` and `smoke` will each take multiple hours to run.  Most of the remaining test groups will run within an hour.

The `smoke` test group is intended to give good general coverage of test scenarios.  The `vape` group is a lightweight version of the `smoke` group.  Note that a 2i capable backend is required for both `smoke` and `vape`.

### Run an individual test

Tests can be run individually, with an additional facility that failing tests will be paused at the point they fail, if they fail.  This allows you to inspect the state of the cluster (within `~/rtdev/riak`) at the failure point.

```bash
# e.g. run verify_crdt_capability with leveled backend
./riak_test -c rtdev -t verify_crdt_capability -b leveled
```

Individual tests will abort on failure, but leave the riak instances running so that you can attach to them via `riak remote_console` and examine the state at the point of the failure.  The logs for each node will be available in the `~/rt/riak/current/dev/dev{n}/riak/log` path. 

When updating a test, run `make all` before re-running the test.

In future versions (`openriak-3.4` and beyond), riak_test changes should also be verified using `./rebar3 as check do xref, dialyzer`.  Validation is not currently supported in `openriak-3.2`.
