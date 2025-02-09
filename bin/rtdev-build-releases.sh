#!/usr/bin/env bash

# You need to use this script once to build a set of stagedevrels for prior
# releases of Riak (for mixed version / upgrade testing). You should
# create a directory and then run this script from within that directory.
# I have ~/test-releases that I created once, and then re-use for testing.
#
# See rtdev-setup-releases.sh as an example of setting up mixed version layout
# for testing.

# Different versions of Riak were released using different Erlang versions,
# make sure to build with the appropriate version.

# This is based on my usage of having multiple Erlang versions in different
# directories. If using kerl or whatever, modify to use kerl's activate logic.
# Or, alternatively, just substitute the paths to the kerl install paths as
# that should work too.
: ${R16B02:=$HOME/.erlang_releases/R16B02-basho10-O2}
: ${R203:=$HOME/.erlang_releases/20.3}

# These are the default tags to use when building basho OTP releases.
# Export different tags to get a different build. N.B. You will need to
# remove the builds from kerl (e.g., kerl delete build $BUILDNAME) and
# possibly remove the directories above.
: ${R16_TAG:="OTP_R16B02_basho10"}
: ${R203_TAG:="20.3"}

# By default the Open Source version of Riak will be used, but for internal
# testing you can override this variable to use `riak_ee` instead
: ${RT_USE_EE:=""}
GITURL_RIAK="git://github.com/basho/riak"
GITURL_RIAK_EE="git@github.com:basho/riak_ee"
GITDIR="riak-src"


checkbuild()
{
    ERLROOT=$1

    if [ ! -d $ERLROOT ]; then
        echo -n " - $ERLROOT cannot be found, install with kerl? [Y|n]: "
        read ans
        if [[ $ans == n || $ans == N ]]; then
            echo
            echo " [ERROR] Can't build $ERLROOT without kerl, aborting!"
            exit 1
        else
            if [ ! -x kerl ]; then
                echo "   - Fetching kerl."
                if [ ! `which curl` ]; then
                    echo "You need 'curl' to be able to run this script, exiting"
                    exit 1
                fi
                curl -O https://raw.githubusercontent.com/spawngrid/kerl/master/kerl > /dev/null 2>&1; chmod a+x kerl
            fi
        fi
    fi
}

kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    export CFLAGS="-g -O2"
    export LDFLAGS="-g"
    if [ -n "`uname -r | grep el6`" ]; then
        export CFLAGS="-g -DOPENSSL_NO_EC=1"
    fi
    BUILDFLAGS="--disable-hipe --enable-smp-support --without-odbc"
    if [ $(uname -s) = "Darwin" ]; then
        export CFLAGS="-g -O0"
        BUILDFLAGS="$BUILDFLAGS --enable-darwin-64bit --with-dynamic-trace=dtrace"
    else
        BUILDFLAGS="$BUILDFLAGS --enable-m64-build"
    fi

    KERL_ENV="KERL_CONFIGURE_OPTIONS=${BUILDFLAGS}"
    MAKE="make -j10"

    echo " - Building Erlang $RELEASE (this could take a while)"
    # Use the Basho-patched version of Erlang 
    BUILD_CMD="./kerl build $RELEASE $BUILDNAME"
    env "$KERL_ENV" "MAKE=$MAKE" $BUILD_CMD
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl build $BUILDNAME failed"
        exit 1
    fi

    echo " - Installing $RELEASE into $HOME/$BUILDNAME"
    ./kerl install $BUILDNAME "$HOME/.erlang_releases_riak_test/$BUILDNAME" > /dev/null 2>&1
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl install $BUILDNAME failed"
        exit 1
    fi
}

build()
{
    SRCDIR=$1
    ERLROOT=$2
    TAG="$3"
    if [ -z $4 ]; then
        LOCKED_DEPS=true
    else
        LOCKED_DEPS=$4
    fi

    if [ -z "$RT_USE_EE" ]; then
        GITURL=$GITURL_RIAK
        GITTAG=$TAG
    else
        GITURL=$GITURL_RIAK_EE
        GITTAG=$TAG
    fi

    echo "Getting sources from github"
    if [ ! -d $GITDIR ]; then
        git clone $GITURL $GITDIR
    else
        cd $GITDIR
        git pull origin develop
        cd ..
    fi

    echo "Building $SRCDIR:"

    checkbuild $ERLROOT
    if [ ! -d $ERLROOT ]; then
        BUILDNAME=`basename $ERLROOT`
        RELEASE=`echo $BUILDNAME | awk -F- '{ print $2 }'`
        kerl $RELEASE $BUILDNAME
    fi

    if [ ! -d $SRCDIR ]
    then
        GITRES=1
        echo " - Cloning $GITURL"
        GITRES=$?
        if [ $GITRES -eq 0 -a -n "$TAG" ]; then
            cd $GITDIR
            git checkout $GITTAG
            GITRES=$?
            cd ..
	    git clone $GITDIR $SRCDIR
        fi
    fi

    if [ ! -f "$SRCDIR/built" ]
    then

        RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"

        echo " - Building devrel in $SRCDIR (this could take a while)"
        cd $SRCDIR

        if $LOCKED_DEPS
        then
            CMD="make devrel"
        else
            CMD="make devrel"
        fi

        $RUN $CMD
        RES=$?
        if [ "$RES" -ne 0 ]; then
            echo "[ERROR] make devrel failed"
            exit 1
        fi
        touch built
        cd ..
        echo " - $SRCDIR built."
    else
        echo " - already built"
    fi
}

#build "riak-2.2.8" $R16B02 riak-2.2.8
build "riak-3.0" $R203 develop-3.0-ns
echo
