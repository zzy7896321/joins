#include <tpie/tpie.h>
#include <tpie/memory.h>
#include <tpie/btree.h>
#include <iostream>
using namespace std;

struct payload {
    int key;
    int value;
};

struct payload_key {
    int operator()(const payload &l) const noexcept { return l.key; }
};

void test_btree() {
    tpie::btree_builder<payload, tpie::btree_key<payload_key>, tpie::btree_internal> builder;
    for (int i = 0; i < 100000; i += 2) {
        builder.push(payload{i, 111});
    }
    auto btree = builder.build();

    cout << btree.size() << endl;
    btree.insert(payload{5001, 112});
    cout << btree.size() << endl;

    auto iter = btree.lower_bound(5001);
    while (iter != btree.end() && iter->key == 5001) {
        cout << iter->value << endl;
        ++iter;
    }
    cout << "<END>" << endl;
}

int main(int argc, char * argv[]) {
    tpie::tpie_init(); 
    tpie::get_memory_manager().set_limit(4ll * 1024 * 1024 * 1024); // 4 GB

    test_btree();

    tpie::tpie_finish(); 

    return 0;
}

