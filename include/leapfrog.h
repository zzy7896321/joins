#ifndef LEAPFROG_H
#define LEAPFROG_H

#include "common.h"
#include <tpie/btree.h>
#include <tpie/file_stream.h>
#include <iostream>
#include <cstdint>
#include <cstdarg>
#include <vector>
#include <string>
#include <algorithm>
using std::uint8_t;

typedef std::uint8_t lf_key_size_type;

struct lf_key_comparator {
    bool operator()(const value_type &l, const value_type &r) const noexcept {
        return (l.subject < r.subject) ||
            (l.subject == r.subject && l.object < r.object);
    }
};

struct lf_key_info {
    lf_key_size_type key1_depth,
                     key2_depth;
};

template<typename ...T>
struct lf_join {
    typedef tpie::btree<value_type, btree_comp<lf_key_comparator>, T...> btree_type;

    std::vector<btree_type> m_btrees;
    std::vector<lf_key_info> m_keyinfo;
    std::vector<std::vector<lf_iter_info>> m_iterinfo;
    uint64_t m_count;
    std::vector<uint64_t> m_pos;
    
    struct lf_iter_ref {
        btree_type::iterator m_iter;
    };

    struct lf_iter_info {
        lf_key_size_type m_table_id,
                         m_key_id;
        lf_iter_ref &m_iter_ref;
        btree_type &m_btree;
        lf_iter_info *m_next_iter_info, *m_prev_iter_info;
        bool m_at_first;

        lf_iterinfo(lf_key_size_type table_id,
                    lf_key_size_type key_id,
                    lf_iter_ref &iter_ref,
                    btree_type &btree)
            : m_table_id(table_id), m_key_id(key_id), m_iter_ref(&iter_ref),
              m_btree(btree),
              m_base_value{iter_ref.m_iter->subject, iter_ref.m_iter->object},
              m_next_iter_info(nullptr), m_prev_iter_info(nullptr),
              m_at_first(true) {}
        
        value_type m_base_value;

        int key() {
            return static_cast<attr_type *>(&*(m_iter_ref.m_iter))[m_key_id];
        }

        void next() {
            if (m_at_first) {
                m_at_first = false;
            } else {
                ++m_iter_ref.m_iter;
            }
        }

        bool atEnd() {
            for (lf_key_size_type i = 0; i <= m_key_id; ++i) {
                if (static_cast<attr_type *>(&*(m_iter_ref.m_iter))[i] != 
                    static_cast<attr_type *>(&m_base_value)[i]) return true;
            }
            return false;
        }

        void seek(attr_type key) {
            static_cast<attr_type *>(&m_base_value)[m_key_id] = key;
            for (lf_key_size_type i = m_key_id + 1; i < 2; ++i) {
                static_cast<attr_type *>(&m_base_value)[i] = 0;
            }
            m_iter_ref.m_iter = m_btree.lower_bound(m_base_value);
            m_at_first = false;
        }

        void open() {
            if (m_next_iter_info) {
                m_next_iter_info->m_base_value = *(m_iter_ref.m_iter);
                m_next_iter_info->m_at_first = true;
            }
        }

        void up() {
            if (m_prev_iter_info) {
                m_next_iter_info->seek(m_next_iter_info->m_base_value[m_key_id - 1] + 1);
                m_next_iter_info->m_at_first = true;
            }
        }
    };

    auto nrels() { return m_btrees.size(); }

    lf_join() {}

    ~lf_join() {}
    
    /* assuming that the file is sorted */
    void load_internal_table(tpie::file_stream<value_type> &in,
                    lf_key_size_type subject_depth,
                    lf_key_size_type object_depth) {
        
        tpie::btree_builder<value_type, btree_comp<lf_key_comparator>, 
            tpie::btree_internal, T...> builder;
        value_type obj;
        while (in.can_read()) {
            obj = in.read();
            builder.push(obj);
        }
        m_btrees.emplace_back(builder.build());
        m_keyinfo.emplace_back(lf_key_info{subject_depth, object_depth});
    }
    
    void load_into_external_table(tpie::file_stream<value_type> &in,
            lf_key_size_type subject_depth,
            lf_key_size_type object_depth,
            std::string path) {
        tpie::btree_builder(value_type, btree_comp<lf_key_comparator>,
                tpie::btree_external, T...> builder(path);
        value_type obj;
        while (in.can_read()) {
            obj = in.read();
            builder.push(obj);
        }
        m_btrees.emplace_back(builder.build());
        m_keyinfo.emplace_back(lf_key_info{subject_depth, object_depth});
    }

private:
    bool prepare_iterinfo() {
        m_iterinfo.clear();
        m_iterinfo.resize(1);
        for (lf_key_size_type table_id = 0; table_id < m_keyinfo.size(); ++table_id) {
            m_iterinfo[0].emplace_back(table_id, ~0U, m_btrees[table_id].begin());
            const lf_key_size_type *a_keyinfo = (const lf_key_size_type *) &keyinfo[table_id];
            lf_iter_info *prev_iter_info = nullptr;
            for (lf_key_size_type i = 0; i < 2; ++i) {
                if (a_keyinfo[i] >= m_iterinfo.size()) {
                    m_iterinfo.resize(a_keyinfo[i] + 1);
                }
                m_iterinfo[a_keyinfo[i]].emplace_back(table_id, i,
                        m_iterinfo[0][table_id].iter);
                m_iterinfo[a_keyinfo[i]].back().m_prev_iter_info = prev_iter_info;
                prev_iter_info->m_next_iter_info = &(m_iterinfo[a_keyinfo[i]].back());
                prev_iter_info = &(m_iterinfo[a_keyinfo[i]].back());
                //if (i == 1) {
                //    m_iterinfo[a_keyinfo[i]].back().m_next_iter_info = 
                //        &(m_iterinfo[0].back());
                //}
            }
        }
        
        std::cerr << "total depth = " << m_iterinfo.size() - 1; << std::endl;
        for (lf_key_size_type depth = 0; depth < m_iterinfo.size(); ++depth) {
            std::cerr << "depth " << depth << ':' ;
            if (m_iterinfo[depth].empty()) return true;
            for (const auto &iterinfo: m_iterinfo[depth]) {
                std::cerr << " {" << iterinfo.table_id << ", "
                    << iterinfo.key_id << "}";
            }
            std::cerr << std::endl;
        }
        return false;
    }
    
    /* @returns atEnd */
    void init(lf_key_size_type depth) {
        for (lf_key_size_type i = 0; i < m_iterinfo[depth].size(); ++i) {
            if (m_iterinfo[depth][i].atEnd()) {
                m_pos.push_back(i);
                return ;
            }
        }
        std::sort(m_iterinfo[i].begin(), m_iterinfo[i].end(),
                [&](const lf_iter_info &l, const lf_iter_info &r) -> bool {
                    return l.key() < r.key();
                }
        );
        m_pos.push_back(0ull);
        search(depth);
    }

    void search(lf_key_size_type depth) {
        auto k = m_iterinfo.size();
        auto p = m_pos.back();
        attr_type max_key = m_iterinfo[depth][(p + k - 1) % k].key();
        for (;;) {
            auto key = m_iterinfo[depth][p].key();
            if (key == max_key) {
                break;
            } else {
                m_iterinfo[depth][p].seek(max_key);
                if (m_iterinfo[depth][p].atEnd()) {
                    break;
                } else {
                    max_key = m_iterinfo[depth][p].key();
                    p = (p + 1) % k;
                }
            }
        }
        m_pos.back() = p;
    }

    void next(lf_key_size_type depth) {
        auto p = m_pos.back();
        m_iterinfo[depth][p].next();
        if (!m_iterinfo[depth][p].atEnd()) {
            p = (p + 1) % k;
            search();
        }
    }

    void do_join() {
        m_pos.clear();
        lf_key_size_type depth = 1;
        while (depth != 0) {
            if (m_pos.length != depth) {
                init(depth);
            } else {
                next(depth);
            }
            auto p = m_pos.back();
            if (m_iterinfo[depth][p].atEnd()) {
                m_pos.pop();
                for (auto &iter_info: m_iterinfo[depth]) {
                    iter_info.up();
                }
                --depth;
            } else {
                if (depth + 1 == m_iterinfo.size()) {
                    ++m_count;
                } else {
                    ++depth;
                    for (auto &iter_info: m_iterinfo[depth]) {
                        iter_info.open();
                    }
                }
            }
        }
    }

public:
    uint64_t join_count() {
        if (prepare_iterinfo()) return 0;
        m_count = 0;
        do_join();
        return m_count;
    }
};

#endif
