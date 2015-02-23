TAR			?= tar
GUNZIP		?= gunzip
HAMSTER_VER = 2.1.10

all: dist

dist: hamsterdb-$(HAMSTER_VER).tar.gz libhamsterdb.a
	#@rm -rf hamsterdb-$(HAMSTER_VER)

hamsterdb-$(HAMSTER_VER).tar.gz:
	wget -nv http://files.hamsterdb.com/dl/hamsterdb-$(HAMSTER_VER).tar.gz

libhamsterdb.a:
	$(GUNZIP) -c hamsterdb-$(HAMSTER_VER).tar.gz | $(TAR) xf -
	cd hamsterdb-$(HAMSTER_VER); CFLAGS="-fPIC" ./configure --without-tcmalloc --disable-remote --disable-encryption
	make -j 4 -C hamsterdb-$(HAMSTER_VER)
	cp hamsterdb-$(HAMSTER_VER)/src/.libs/libhamsterdb.a .
	mkdir include; cp -R hamsterdb-$(HAMSTER_VER)/include/ham include

clean:
	rm -rf hamsterdb-$(HAMSTER_VER) include
	rm -f libhamsterdb.a
