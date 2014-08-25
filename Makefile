#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

#
# Tools
#
NPM		:= $(shell which npm)
TAP		:= ./node_modules/.bin/tap

#
# Files
#
JS_FILES	:= $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -o indent=2,doxygen,unparenthesized-return=0

#
# Repo-specific targets
#
.PHONY: all
all: test check

.PHONY: setup
setup: $(NPM)
	$(NPM) install

.PHONY: test
test: setup $(TAP)
	$(TAP) test/*.test.js

.PHONY: coverage
coverage: $(TAP)
	TAP_COV=1 $(TAP) test/*.test.js

include ./Makefile.deps
include ./Makefile.targ
