# Instructions to install RegMAS on Debian/Ubuntu

RegMAS is, unsurprisingly, not in anyone's repos, so you must build it from source and patch it by hand:

```
sudo apt-get install qt-sdk glpk zlib1g-dev #get the build depends
wget http://regmas.org/snapshots/regmas_daily.tgz #get the bleeding edge
tar xvzf regmas_daily.tgz
cd regmas
nano src/main.cpp #comment out documentation line in src/main.cpp (line 33) (???!?!?!)
qmake
make #do you do make twice???
#MAYBE: add "-lz" to end of line starting with "LIBS =" in src/Makefile
make
./regmas
```
