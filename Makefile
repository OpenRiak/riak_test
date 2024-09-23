# -------------------------------------------------------------------
#
# Copyright (c) 2012-2016 Basho Technologies, Inc.
# Copyright (c) 2017-2023 Workday, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------
#
# Mostly just a façade over rebar3 from the command line.
#

REBAR ?= ./rebar3

all: escript

docs:
	$(REBAR) edoc

docsclean:
	@$(RM) -rf doc/*.png doc/*.html doc/*.css edoc-info

escript:
	$(REBAR) as prod compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean
	$(RM) -rf riak_test _build

quickbuild: compile

xref:
	$(REBAR) as check xref

dialyzer:
	$(REBAR) as check dialyzer
