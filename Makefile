all: compile

compile:
	./rebar compile

clean:
	./rebar clean

doc:
	./rebar doc

dialyzer:
	dialyzer -Wrace_conditions --src src

test:
	LD_LIBRARY_PATH=priv erl -pa ebin -s ham_eqc2 test -s init stop

benchmark: compile
	@erl -noshell -pa ebin -eval 'hammy_bench:start().' -s init stop
