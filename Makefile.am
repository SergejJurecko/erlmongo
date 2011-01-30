EXTRA_DIST = \
	erlmongo.spec \
	README.rdoc \
	LICENSE.txt \
	$(source_files) \
	ebin/erlmongo.app

erlmongohdrdir = $(ERLANG_INSTALL_LIB_DIR_erlmongo)/include
dist_erlmongohdr_DATA = \
	src/erlmongo.hrl

erlmongobindir = $(ERLANG_INSTALL_LIB_DIR_erlmongo)/ebin

source_files = \
	src/erlmongo_app.erl \
	src/mongoapi.erl \
	src/mongodb.erl \
	src/mongodb_supervisor.erl

beam_files = $(foreach B,$(notdir $(source_files:.erl=.beam)),ebin/$(B))

erlmongobin_DATA = \
	$(beam_files) \
	ebin/erlmongo.app

CLEANFILES = $(beam_files)

all: $(beam_files);

ebin/%.beam : src/%.erl
	test -d ebin || mkdir ebin
	$(ERLC) $(ERLC_FLAGS) -o ebin $<

