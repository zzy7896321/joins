#include "leapfrog.h"
#include <tpie/tpie.h>
#include <tpie/memory.h>
#include <tpie/btree.h>
#include <tpie/file_stream.h>
#include <tpie/sort.h>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include <unistd.h>
#include <tuple>
#include <fstream>
#include <cstdio>
using namespace std;

typedef tuple<attr_type, attr_type, attr_type> triple_t;

struct dictionary_t {
    vector<string> mapping;
    unordered_map<string, attr_type> inverted_index;

    dictionary_t() {}
    dictionary_t(dictionary_t &&dict):
        mapping(move(dict.mapping)),
        inverted_index(move(dict.inverted_index)) {}
    dictionary_t(const dictionary_t &dict):
        mapping(dict.mapping),
        inverted_index(dict.inverted_index) {}

    dictionary_t &operator=(dictionary_t &&dict) {
        mapping = move(dict.mapping);
        inverted_index = move(dict.inverted_index);
        return *this;
    }
    dictionary_t &operator=(const dictionary_t &dict) {
        mapping = dict.mapping;
        inverted_index = dict.inverted_index;
        return *this;
    }

    dictionary_t &add(const string &s) {
        if (inverted_index.find(s) == inverted_index.end()) {
            mapping.push_back(s);
            inverted_index.emplace(s, mapping.size() - 1);
        }
        return *this;
    }

    void rebuild_inverted_index() {
        inverted_index.clear();
        for (vector<string>::size_type i = 0; i != mapping.size(); ++i) {
            inverted_index.emplace(mapping[i], i);
        }
    }

    attr_type lookup(const string &s) const {
        return inverted_index.at(s);
    }
};

bool parse_turtle(const string &line, tuple<string, string, string> &tuple) {
    auto p1 = line.find('<'); 
    if (p1 == string::npos) return false;
    auto p2 = line.find('>', p1 + 1);
    if (p2 == string::npos) return false;
    auto p3 = line.find('<', p2 + 1);
    if (p3 == string::npos) return false;
    auto p4 = line.find('>', p3 + 1);
    if (p4 == string::npos) return false;
    auto p5 = line.find_first_of("<\"", p4 + 1);
    if (p5 == string::npos) return false;
    auto p6 = (line[p5] == '<') ? line.find('>', p5 + 1) :
        line.find('"', p5 + 1);
    if (p6 == string::npos) return false;
    auto p7 = line.find('.', p6 + 1);
    if (p7 == string::npos || p7 + 1 != line.length()) return false;
    tuple = make_tuple(line.substr(p1, p2 - p1 + 1),
            line.substr(p3, p4 - p3 + 1),
            line.substr(p5, p6 - p5 + 1));
    return true;
}

bool load_dictionary(string data_dir, dictionary_t &out_dict) {
    dictionary_t dict;
    
    ifstream dict_file(data_dir + "/dictionary.txt");
    if (!dict_file.good()) return false;
    string line;
    getline(dict_file, line);
    auto dict_size = stoull(line);
    dict.mapping.reserve(dict_size);
    while (getline(dict_file, line)) {
        dict.mapping.emplace_back(move(line));
    }
    dict.rebuild_inverted_index();

    out_dict = move(dict);
    return true;
}

bool create_dictionary(string data_dir, dictionary_t &out_dict) {
    dictionary_t dict;
    
    ifstream file_list(data_dir + "/file_list.txt");
    if (!file_list.good()) return false;
    string file_name;
    string turtle;
    tuple<string, string, string> spo;
    while (getline(file_list, file_name), !file_name.empty()) {
        ifstream ttl_file(data_dir + "/" + file_name);
        if (!ttl_file.good()) return false;
        while (getline(ttl_file, turtle)) {
            decltype(turtle.length()) p = 0;
            while (p < turtle.length() && turtle[p] == ' ') ++p;
            if (p == turtle.length() || turtle[p] == '#') continue;
            if (!parse_turtle(turtle, spo)) return false;
            dict.add(get<0>(spo)).add(get<1>(spo)).add(get<2>(spo));        
        }
    }

    ofstream dict_file(data_dir + "/dictionary.txt");
    dict_file << dict.mapping.size() << endl;
    for (const auto &s: dict.mapping) {
        dict_file << s << endl;
    }

    out_dict = move(dict);
    return true;
}

bool load_or_create_dictionary(string data_dir, dictionary_t &out_dict) {
    if (access((data_dir + "/dictionary.txt").c_str(), F_OK)) {
        return create_dictionary(data_dir, out_dict);
    } else {
        return load_dictionary(data_dir, out_dict);
    }
}

bool read_and_sort_by_predicate(string data_dir, const dictionary_t &dict) {
    ifstream file_list(data_dir + "/file_list.txt");
    if (!file_list.good()) return false;
    
    cerr << "concatenating files ..." << endl;
    tpie::file_stream<triple_t> out;
    out.open(data_dir + "/sorted_by_predicate.dat");

    string ttl_file_name;
    string turtle;
    tuple<string, string, string> spo;
    triple_t triple;
    while (getline(file_list, ttl_file_name), !ttl_file_name.empty()) {
        ifstream ttl_file(data_dir + "/" + ttl_file_name);
        if (!ttl_file.good()) return false;
        while (getline(ttl_file, turtle)) {
            decltype(turtle.length()) p = 0;
            while (p < turtle.length() && turtle[p] == ' ') ++p;
            if (p == turtle.length() || turtle[p] == '#') continue;
            if (!parse_turtle(turtle, spo)) return false;
            triple = make_tuple(dict.lookup(get<1>(spo)),
                        dict.lookup(get<0>(spo)),
                        dict.lookup(get<2>(spo)));
            out.write(triple);
        }
    }
    
    cerr << "sorting ..." << endl;
    tpie::sort(out, out);

    return true;
}

bool create_partitioned_tables(string data_dir, const dictionary_t &dict) {
    cerr << "creating partitioned tables ..." << endl;
    tpie::file_stream<triple_t> in;
    tpie::file_stream<pair<attr_type, attr_type>> out, outr;
    in.open(data_dir + "/sorted_by_predicate.dat", tpie::access_read);
    vector<attr_type> predicates;
    
    triple_t triple;
    if (!in.can_read()) {
        return false;
    }
    triple = in.read();
    predicates.push_back(get<0>(triple));
    out.open(data_dir + "/" + to_string(predicates.back()) + ".dat", tpie::access_write);
    out.write(make_pair(get<1>(triple), get<2>(triple)));
    outr.open(data_dir + "/" + to_string(predicates.back()) + "r.dat", tpie::access_write);
    outr.write(make_pair(get<2>(triple), get<1>(triple)));
    while (in.can_read()) {
        triple = in.read();
        attr_type predicate = get<0>(triple);
        if (predicate != predicates.back()) {
            out.close();
            tpie::sort(outr, outr);
            outr.close();
            predicates.push_back(predicate);
            out.open(data_dir + "/" + to_string(predicates.back()) + ".dat", tpie::access_write);
            outr.open(data_dir + "/" + to_string(predicates.back()) + "r.dat", tpie::access_write);
        }
        out.write(make_pair(get<1>(triple), get<2>(triple)));
        outr.write(make_pair(get<2>(triple), get<1>(triple)));
    }
    out.close();
    tpie::sort(outr, outr);
    outr.close();

    ofstream predicate_list(data_dir  + "/predicate_list.txt");
    predicate_list << predicates.size() << endl;
    for (auto predicate: predicates) {
        predicate_list << predicate << ' ' << dict.mapping[predicate] << endl;
    }
    
    return true;
}

bool check_or_transform_turtle(string data_dir, const dictionary_t &dict) {
    if (access((data_dir + "/predicate_list.txt").c_str(), F_OK)) {
        if (access((data_dir + "/sorted_by_predicate.dat").c_str(), F_OK)) {
            if (!read_and_sort_by_predicate(data_dir, dict)) {
                return false;
            }
        }
        if (!create_partitioned_tables(data_dir, dict)) {
            return false;
        }
    }
    return true;
}

void remove_files(string data_dir) {
    remove((data_dir + "/dictionary.txt").c_str());
    remove((data_dir + "/sorted_by_predicate.dat").c_str());
    ifstream predicate_list(data_dir + "/predicate_list.txt");
    string line;
    getline(predicate_list, line);
    while (getline(predicate_list, line), !line.empty()) {
        auto p = line.find(' ');
        remove((data_dir + "/" + line.substr(0, p) + ".dat").c_str());
        remove((data_dir + "/" + line.substr(0, p) + "r.dat").c_str());
    }
    predicate_list.close();
    remove((data_dir + "/predicate_list.txt").c_str());
}

void usage(char *progname) {
    cout  << "usage: " << progname << " [-f] <data_dir> <mem_limit (GB)>" << endl;
}

void run_query(string data_dir, const dictionary_t &dict) {
    ifstream query(data_dir + "/query.txt");
    if (!query.good()) return ;
    
    lf_join<tpie::btree_internal> join;
    string line;
    while (getline(query, line), !line.empty()) {
        auto p = line.find(' ');
        auto subject_depth = stoull(line.substr(0, p));
        auto p2 = line.find(' ', p + 1);
        auto object_depth = stoull(line.substr(p + 1, p2 - p - 1));
        auto predicate_str = line.substr(p2 + 1);
        auto predicate = dict.lookup(predicate_str);

        tpie::file_stream<value_type> in;
        if (subject_depth < object_depth) {
            in.open(data_dir + "/" + to_string(predicate) + ".dat", tpie::access_read);
            join.load_internal_table(in, subject_depth, object_depth);
        } else {
            in.open(data_dir + "/" + to_string(predicate) + "r.dat", tpie::access_read);
            join.load_internal_table(in, object_depth, subject_depth);
        }
    }

    auto count = join.join_count();
    cout << "count = " << count << endl;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }
    
    int argi = 1;
    bool force_rebuild = false;
    if (!strcmp(argv[argi], "-f")) {
        ++argi;
        force_rebuild = true;
    }
    const string data_dir = argv[argi++];
    if (argi == argc) {
        usage(argv[0]);
        return 1;
    }
    const unsigned long long mem_limit = stoull(argv[argi++]) * 1024 * 1024 * 1024;

    tpie::tpie_init();
    tpie::get_memory_manager().set_limit(mem_limit);

    if (force_rebuild) {
        remove_files(data_dir);
    }

    dictionary_t dict;
    if (!load_or_create_dictionary(data_dir, dict)) {
        cout << "[ERROR] load dictionary" << endl;
        return 1;
    }

    if (!check_or_transform_turtle(data_dir, dict)) {
        cout << "[ERROR] transform turtle" << endl;
        return 1;
    }

    run_query(data_dir, dict);

    tpie::tpie_finish();
    cout << "<DONE>" << endl;

    return 0;

}
