.PHONY: test

all: compile

compile:
	./rebar compile

clean:
	./rebar clean

doc:
	./rebar doc

dialyzer: compile
	dialyzer -Wrace_conditions --src src

test eunit: compile
	./rebar eunit
	# LD_LIBRARY_PATH=priv erl -pa ebin -s ham_eqc2 test -s init stop

shell: compile
	LD_LIBRARY_PATH=priv erl -pa ebin
