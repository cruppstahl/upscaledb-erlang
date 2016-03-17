.PHONY: test doc

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

eqc: compile
	./rebar eqc

shell: compile
	LD_LIBRARY_PATH=priv erl -pa ebin
