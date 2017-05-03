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
#include <memory>
using std::uint8_t;

typedef std::uint8_t lf_key_size_type;

struct lf_key_comparator {
    bool operator()(const value_type &l, const value_type &r) const noexcept {
        return (l.key1 < r.key1) ||
            (l.key1 == r.key1 && l.key2 < r.key2);
    }
};

struct lf_key_info {
    lf_key_size_type key1_depth,
                     key2_depth;
};

template<typename ...T>
struct lf_join {
    typedef tpie::btree<value_type, tpie::btree_comp<lf_key_comparator>, T...> btree_type;
    
    struct lf_iter_ref {
        typename btree_type::iterator m_iter;
    };

    struct lf_iter_info {
        lf_key_size_type m_table_id,
                         m_key_id;
        std::shared_ptr<lf_iter_ref> m_iter_ref;
        btree_type *m_btree;
        lf_iter_info *m_next_iter_info, *m_prev_iter_info;
        value_type m_base_value;
        
        lf_iter_info(lf_key_size_type table_id, 
                lf_key_size_type key_id,
                typename btree_type::iterator iter,
                btree_type *btree):
            lf_iter_info(table_id, key_id, std::shared_ptr<lf_iter_ref>(new lf_iter_ref{iter}), btree) {
            
        }

        lf_iter_info(lf_key_size_type table_id,
                    lf_key_size_type key_id,
                    std::shared_ptr<lf_iter_ref> iter_ref,
                    btree_type *btree)
            : m_table_id(table_id), m_key_id(key_id),
              m_iter_ref(iter_ref), m_btree(btree),
              m_base_value{iter_ref->m_iter->key1, iter_ref->m_iter->key2},
              m_next_iter_info(nullptr), m_prev_iter_info(nullptr) {}


        int key() const noexcept {
            return reinterpret_cast<const attr_type *>(&*(m_iter_ref->m_iter))[m_key_id];
        }

        void next() {
            seek(key() + 1);
        }

        bool atEnd() const noexcept {
            for (lf_key_size_type i = 0; i <= m_key_id; ++i) {
                if (reinterpret_cast<const attr_type *>(&*(m_iter_ref->m_iter))[i] != 
                    reinterpret_cast<const attr_type *>(&m_base_value)[i]) return true;
            }
            return false;
        }

        void seek(attr_type key) {
            reinterpret_cast<attr_type *>(&m_base_value)[m_key_id] = key;
            for (lf_key_size_type i = m_key_id + 1; i < 2; ++i) {
                reinterpret_cast<attr_type *>(&m_base_value)[i] = 0;
            }
            m_iter_ref->m_iter = m_btree->lower_bound(m_base_value);
        }

        void open() {
            if (m_next_iter_info) {
                m_next_iter_info->m_base_value = *(m_iter_ref->m_iter);
            }
        }

        void up() {
            /*if (m_prev_iter_info) {
                m_prev_iter_info->seek(
                    reinterpret_cast<attr_type *>(&m_prev_iter_info->m_base_value)[m_key_id - 1] + 1);
                m_prev_iter_info->m_at_first = true;
            } */
            if (m_prev_iter_info) {
                m_prev_iter_info->seek(
                    reinterpret_cast<attr_type *>(&m_base_value)[m_key_id - 1]);
            }
        }
    };

    static constexpr bool is_internal =
        tpie::bbits::tree_state<value_type, 
            typename tpie::bbits::OptComp<T...>::type>::is_internal;

    std::vector<btree_type> m_btrees;
    std::vector<lf_key_info> m_keyinfo;
    std::vector<std::vector<lf_iter_info*>> m_iterinfo;
    uint64_t m_count;
    std::vector<uint64_t> m_pos;

    auto nrels() { return m_btrees.size(); }

    lf_join() {}

    ~lf_join() {
        for (auto &v: m_iterinfo) {
            for (auto iterinfo: v) {
                delete iterinfo;
            }
        }
    }
    
    /* assuming that the file is sorted */
    template <typename X=tpie::bbits::enab>
    void load_internal_table(tpie::file_stream<value_type> &in,
                    lf_key_size_type subject_depth,
                    lf_key_size_type object_depth,
                    tpie::bbits::enable<X, is_internal> = tpie::bbits::enab()) {
        
        tpie::btree_builder<value_type, tpie::btree_comp<lf_key_comparator>,
            T...> builder;
        value_type obj;
        while (in.can_read()) {
            obj = in.read();
            builder.push(obj);
        }
        m_btrees.emplace_back(builder.build());
        m_keyinfo.emplace_back(lf_key_info{subject_depth, object_depth});
    }
    
    template <typename X=tpie::bbits::enab>
    void load_into_external_table(tpie::file_stream<value_type> &in,
            lf_key_size_type subject_depth,
            lf_key_size_type object_depth,
            std::string path,
            tpie::bbits::enable<X, !is_internal> = tpie::bbits::enab()) {
        tpie::btree_builder<value_type, tpie::btree_comp<lf_key_comparator>,
                T...> builder(path);
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
            m_iterinfo[0].emplace_back(new lf_iter_info(table_id, (lf_key_size_type) ~0U,
                    m_btrees[table_id].begin(), &m_btrees[table_id]));
            const lf_key_size_type *a_keyinfo = (const lf_key_size_type *) &m_keyinfo[table_id];
            lf_iter_info *prev_iter_info = nullptr;
            for (lf_key_size_type i = 0; i < 2; ++i) {
                if (a_keyinfo[i] >= m_iterinfo.size()) {
                    m_iterinfo.resize(a_keyinfo[i] + 1);
                }
                m_iterinfo[a_keyinfo[i]].emplace_back(new lf_iter_info(table_id, i,
                        m_iterinfo[0][table_id]->m_iter_ref, &m_btrees[table_id]));
                m_iterinfo[a_keyinfo[i]].back()->m_prev_iter_info = prev_iter_info;
                if (prev_iter_info)
                    prev_iter_info->m_next_iter_info = m_iterinfo[a_keyinfo[i]].back();
                prev_iter_info = m_iterinfo[a_keyinfo[i]].back();
            }
        }
        
        std::cerr << "total depth = " << m_iterinfo.size() - 1 << std::endl;
        for (lf_key_size_type depth = 0; depth < m_iterinfo.size(); ++depth) {
            std::cerr << "depth " << (unsigned) depth << ':';
            if (m_iterinfo[depth].empty()) return true;
            for (const auto &iterinfo: m_iterinfo[depth]) {
                std::cerr << " {" << (unsigned) iterinfo->m_table_id << ", "
                    << (unsigned) iterinfo->m_key_id << "}";
            }
            std::cerr << std::endl;
        }
        return false;
    }
    
    /* @returns atEnd */
    void init(lf_key_size_type depth) {
        for (lf_key_size_type i = 0; i < m_iterinfo[depth].size(); ++i) {
            if (m_iterinfo[depth][i]->atEnd()) {
                m_pos.push_back(i);
                return ;
            }
        }
        std::sort(m_iterinfo[depth].begin(), m_iterinfo[depth].end(),
                [&](const lf_iter_info *l, const lf_iter_info *r) -> bool {
                    return l->key() < r->key();
                }
        );
        m_pos.push_back(0ull);
        search(depth);
    }

    void search(lf_key_size_type depth) {
        auto k = m_iterinfo[depth].size();
        auto p = m_pos.back();
        attr_type max_key = m_iterinfo[depth][(p + k - 1) % k]->key();
        for (;;) {
            auto key = m_iterinfo[depth][p]->key();
            if (key == max_key) {
                break;
            } else {
                m_iterinfo[depth][p]->seek(max_key);
                if (m_iterinfo[depth][p]->atEnd()) {
                    break;
                } else {
                    max_key = m_iterinfo[depth][p]->key();
                    p = (p + 1) % k;
                }
            }
        }
        m_pos.back() = p;
    }

    void next(lf_key_size_type depth) {
        auto k = m_iterinfo[depth].size();
        auto p = m_pos.back();
        m_iterinfo[depth][p]->next();
        if (!m_iterinfo[depth][p]->atEnd()) {
            m_pos.back() = (p + 1) % k;
            search(depth);
        }
    }

    void do_join() {
        m_pos.clear();
        lf_key_size_type depth = 1;
        while (depth != 0) {
            if (m_pos.size() != depth) {
                init(depth);
            } else {
                next(depth);
            }
            auto p = m_pos.back();
            if (m_iterinfo[depth][p]->atEnd()) {
                m_pos.pop_back();
                for (auto iter_info: m_iterinfo[depth]) {
                    iter_info->up();
                }
                --depth;
            } else {
                if (depth + 1 == m_iterinfo.size()) {
                    for (lf_key_size_type i = 1; i < m_iterinfo.size(); ++i) {
                        std::cout << m_iterinfo[i][0]->key() << ' ';
                    }
                    std::cout << std::endl;
                    ++m_count;
                } else {
                    ++depth;
                    for (auto iter_info: m_iterinfo[depth]) {
                        iter_info->open();
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
