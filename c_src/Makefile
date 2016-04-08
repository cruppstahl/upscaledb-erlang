TAR			?= tar
GUNZIP		?= gunzip
VERSION     = 2.2.0

all: dist

dist: upscaledb-$(VERSION).tar.gz libupscaledb.a
	#@rm -rf upscaledb-$(VERSION)

upscaledb-$(VERSION).tar.gz:
	#cp -r ~/prj/upscaledb .
	#mv upscaledb upscaledb-2.2.0
	#cd upscaledb-2.2.0 && git checkout topic/2.2.0 && cd ..
	wget -nv http://files.upscaledb.com/dl/upscaledb-$(VERSION).tar.gz

libupscaledb.a:
	$(GUNZIP) -c upscaledb-$(VERSION).tar.gz | $(TAR) xf -
	cd upscaledb-$(VERSION); CFLAGS="-fPIC -O3 -DNDEBUG" ./configure --without-tcmalloc --disable-remote --disable-encryption
	make -j 4 -C upscaledb-$(VERSION)
	cp upscaledb-$(VERSION)/src/.libs/libupscaledb.a .
	mkdir include; cp -R upscaledb-$(VERSION)/include/ups include

clean:
	rm -rf upscaledb-$(VERSION) include
	rm -f libupscaledb.a
