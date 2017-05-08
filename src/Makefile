SHELL=/bin/bash
CXX=g++ -std=gnu++14 -O2
CXXFLAGS=-pthread -I../tpie/build.release/install/include -L../tpie/build.release/install/lib -I../include
LDFLAGS=-ltpie -lboost_system -lboost_filesystem

.PHONY: all clean gen_gitignore

TARGETS = bt_example leapfrog

all: $(TARGETS)

bt_example: bt_example.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

leapfrog: leapfrog.cpp ../include/leapfrog.h ../include/common.h
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

gitignore:
	rm -f .gitignore
	echo "*.o" > .gitignore 
	for i in $(TARGETS); do echo $$i >> .gitignore; done

clean:
	rm -f $(TARGETS)
