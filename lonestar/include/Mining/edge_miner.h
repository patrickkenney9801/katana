#ifndef EDGE_MINER_H
#define EDGE_MINER_H
#include "miner.h"

typedef std::set<int> IntSet;
typedef std::vector<bool> BoolVec;
typedef std::vector<IntSet> IntSets;
//typedef galois::gstl::Set<int> IntSet;
//typedef galois::gstl::Vector<bool> BoolVec;
//typedef galois::gstl::Vector<IntSet> IntSets;
typedef std::pair<BoolVec,IntSets> DomainSupport;
typedef QuickPattern<EdgeEmbedding, ElementType> QPattern;
typedef CanonicalGraph<EdgeEmbedding, ElementType> CPattern;
///*
typedef std::pair<unsigned, unsigned> InitPattern;
typedef std::map<InitPattern, DomainSupport> InitMap;
typedef std::unordered_map<QPattern, Frequency> QpMapFreq; // mapping quick pattern to its frequency
typedef std::unordered_map<CPattern, Frequency> CgMapFreq; // mapping canonical pattern to its frequency
typedef std::unordered_map<QPattern, DomainSupport> QpMapDomain; // mapping quick pattern to its domain support
typedef std::unordered_map<CPattern, DomainSupport> CgMapDomain; // mapping canonical pattern to its domain support
typedef std::unordered_map<unsigned, unsigned> FreqMap;
typedef std::unordered_map<unsigned, bool> DomainMap;
//*/
/*
typedef galois::gstl::Map<QPattern, Frequency> QpMapFreq; // mapping quick pattern to its frequency
typedef galois::gstl::Map<CPattern, Frequency> CgMapFreq; // mapping canonical pattern to its frequency
typedef galois::gstl::Map<QPattern, DomainSupport> QpMapDomain; // mapping quick pattern to its domain support
typedef galois::gstl::Map<CPattern, DomainSupport> CgMapDomain; // mapping canonical pattern to its domain support
typedef galois::gstl::Map<unsigned, unsigned> FreqMap;
typedef galois::gstl::Map<unsigned, bool> DomainMap;
//*/
typedef galois::substrate::PerThreadStorage<InitMap> LocalInitMap;
typedef galois::substrate::PerThreadStorage<QpMapFreq> LocalQpMapFreq;
typedef galois::substrate::PerThreadStorage<CgMapFreq> LocalCgMapFreq;
typedef galois::substrate::PerThreadStorage<QpMapDomain> LocalQpMapDomain;
typedef galois::substrate::PerThreadStorage<CgMapDomain> LocalCgMapDomain;

class EdgeMiner : public Miner {
public:
	EdgeMiner(Graph *g) { graph = g; construct_edgemap(); }
	virtual ~EdgeMiner() {}
	// given an embedding, extend it with one more edge, and if it is not automorphism, insert the new embedding into the task queue
	void expand_edge(unsigned max_size, EdgeEmbeddingQueue &in_queue, EdgeEmbeddingQueue &out_queue) {
		//if (show) std::cout << "\n----------------------------- Expanding -----------------------------\n";
		if (show) std::cout << "\n------------------------- Step 1: Expanding -------------------------\n";
		// for each embedding in the worklist, do the edge-extension operation
		galois::do_all(galois::iterate(in_queue),
			[&](const EmbeddingT& emb) {
				unsigned n = emb.size();
				// get the number of distinct vertices in the embedding
				VertexSet vert_set;
				if (n > 3)
					for (unsigned i = 0; i < n; i ++) vert_set.insert(emb.get_vertex(i));
				// for each vertex in the embedding
				for (unsigned i = 0; i < n; ++i) {
					VertexId src = emb.get_vertex(i);
					// make sure each distinct vertex is expanded only once
					if (emb.get_key(i) == 0) {
						// try edge extension
						for (auto e : graph->edges(src)) {
							GNode dst = graph->getEdgeDst(e);
							BYTE existed = 0;
							#ifdef ENABLE_LABEL
							if (is_frequent_edge[*e])
							#endif
								// check if this is automorphism
								if (!is_edge_automorphism(n, max_size, emb, i, src, dst, existed, vert_set)) {
									auto dst_label = 0, edge_label = 0;
									#ifdef ENABLE_LABEL
									dst_label = graph->getData(dst);
									//edge_label = graph->getEdgeData(e); // TODO: enable this for FSM
									#endif
									ElementType new_element(dst, (BYTE)existed, edge_label, dst_label, (BYTE)i);
									EdgeEmbedding new_emb(emb);
									new_emb.push_back(new_element);
									// insert the new (extended) embedding into the worklist
									out_queue.push_back(new_emb);
								}
						}
					}
				}
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(), galois::no_conflicts(),
			galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("Expanding")
		);
	}
	inline unsigned init_aggregator() {
		init_map.clear();
		for (auto src : *graph) {
			auto& src_label = graph->getData(src);
			for (auto e : graph->edges(src)) {
				GNode dst = graph->getEdgeDst(e);
				//auto elabel = graph.getEdgeData(e);
				auto& dst_label = graph->getData(dst);
				if (src_label <= dst_label) {
					InitPattern key = get_init_pattern(src_label, dst_label);
					if (init_map.find(key) == init_map.end()) {
						init_map[key].first.resize(2);
						std::fill(init_map[key].first.begin(), init_map[key].first.end(), 0);
						init_map[key].second.resize(2);
					}
					init_map[key].second[0].insert(src);
					init_map[key].second[1].insert(dst);
					for (unsigned i = 0; i < 2; i ++) {
						if (init_map[key].second[i].size() >= threshold) {
							init_map[key].first[i] = 1;
							init_map[key].second[i].clear();
						}
					}
				}
			}
		}
		std::cout << "Number of single-edge patterns: " << init_map.size() << "\n";
		unsigned count = 0;
		for (auto it = init_map.begin(); it != init_map.end(); ++it)
			if (get_support(it->second)) count ++;
		return count; // return number of frequent single-edge patterns
	}
	inline void quick_aggregate(EdgeEmbeddingQueue &queue, QpMapFreq &qp_map) {
		for (auto emb : queue) {
			QPattern qp(emb);
			if (qp_map.find(qp) != qp_map.end()) {
				qp_map[qp] += 1;
				qp.clean();
			} else qp_map[qp] = 1;
		}
	}
	// aggregate embeddings into quick patterns
	inline void quick_aggregate_each(const EdgeEmbedding& emb, QpMapFreq& qp_map) {
		// turn this embedding into its quick pattern
		QPattern qp(emb);
		// update frequency for this quick pattern
		if (qp_map.find(qp) != qp_map.end()) {
			// if this quick pattern already exists, increase its count
			qp_map[qp] += 1;
			qp.clean();
		// otherwise add this quick pattern into the map, and set the count as one
		} else qp_map[qp] = 1;
	}
/*
	inline void quick_aggregate(EdgeEmbeddingQueue& queue, QpMapFreq& qp_map) {
		galois::do_all(galois::iterate(queue),
			[&](const EdgeEmbedding &emb) {
				QPattern qp(emb);
				if (qp_map.find(qp) != qp_map.end()) {
					qp_map[qp] += 1;
					#ifdef ENABLE_LABEL
					emb.set_qpid(qp.get_id());
					#endif
					qp.clean();
				} else {
					qp_map[qp] = 1;
					#ifdef ENABLE_LABEL
					emb.set_qpid(qp.get_id());
					#endif
				}
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
			galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("QuickAggregation")
		);
	}
//*/
	inline void quick_aggregate_each(EdgeEmbedding& emb, QpMapFreq& qp_map) {
		QPattern qp(emb);
		if (qp_map.find(qp) != qp_map.end()) {
			qp_map[qp] += 1;
			emb.set_qpid(qp.get_id());
			qp.clean();
		} else {
			qp_map[qp] = 1;
			emb.set_qpid(qp.get_id());
		}
	}
	inline void quick_aggregate(EdgeEmbeddingQueue &queue, QpMapDomain &qp_map) {
		for (auto emb : queue) {
			QPattern qp(emb);
			if (qp_map.find(qp) != qp_map.end()) {
				for (unsigned i = 0; i < emb.size(); i ++)
					qp_map[qp].second[i].insert(emb.get_vertex(i));
				qp.clean();
			} else {
				qp_map[qp].second.resize(emb.size());
				for (unsigned i = 0; i < emb.size(); i ++)
					qp_map[qp].second[i].insert(emb.get_vertex(i));
			}
		}
	}
/*
	inline void quick_aggregate(EdgeEmbeddingQueue& queue, QpMapDomain& qp_map) {
		galois::do_all(galois::iterate(queue),
			[&](EdgeEmbedding &emb) {
				unsigned n = emb.size();
				QPattern qp(emb);
				bool qp_existed = false;
				auto it = qp_map.find(qp);
				if (it == qp_map.end()) {
					qp_map[qp].first.resize(n);
					std::fill(qp_map[qp].first.begin(), qp_map[qp].first.end(), 0);
					qp_map[qp].second.resize(n);
					emb.set_qpid(qp.get_id());
				} else {
					qp_existed = true;
					emb.set_qpid((it->first).get_id());
				}
				for (unsigned i = 0; i < n; i ++) {
					if (qp_map[qp].first[i] == 0) {
						qp_map[qp].second[i].insert(emb.get_vertex(i));
						if (qp_map[qp].second[i].size() >= threshold) {
							qp_map[qp].first[i] = 1;
							qp_map[qp].second[i].clear();
						}
					}
				}
				if (qp_existed) qp.clean();
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
			galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("QuickAggregation")
		);
	}
//*/
	inline void quick_aggregate_each(EdgeEmbedding& emb, QpMapDomain& qp_map) {
		unsigned n = emb.size();
		QPattern qp(emb);
		bool qp_existed = false;
		auto it = qp_map.find(qp);
		if (it == qp_map.end()) {
			qp_map[qp].first.resize(n);
			std::fill(qp_map[qp].first.begin(), qp_map[qp].first.end(), 0);
			qp_map[qp].second.resize(n);
			emb.set_qpid(qp.get_id());
		} else {
			qp_existed = true;
			emb.set_qpid((it->first).get_id());
		}
		for (unsigned i = 0; i < n; i ++) {
			if (qp_map[qp].first[i] == 0) {
				qp_map[qp].second[i].insert(emb.get_vertex(i));
				if (qp_map[qp].second[i].size() >= threshold) {
					qp_map[qp].first[i] = 1;
					qp_map[qp].second[i].clear();
				}
			}
		}
		if (qp_existed) qp.clean();
	}
	void canonical_aggregate(const QpMapFreq &qp_map, CgMapFreq &cg_map) {
		for (auto it = qp_map.begin(); it != qp_map.end(); ++it) {
			QPattern qp = it->first;
			unsigned freq = it->second;
			CPattern cg(qp);
			qp.clean();
			if (cg_map.find(cg) != cg_map.end()) cg_map[cg] += freq;
			else cg_map[cg] = freq;
			cg.clean();
		}
	}
	// aggregate quick patterns into canonical patterns.
	inline void canonical_aggregate_each(std::pair<QPattern, Frequency> element, CgMapFreq &cg_map) {
		// turn the quick pattern into its canonical pattern
		CPattern cg(element.first);
		element.first.clean();
		// if this pattern already exists, increase its count
		if (cg_map.find(cg) != cg_map.end()) cg_map[cg] += element.second;
		// otherwise add this pattern into the map, and set the count as 'freq'
		else cg_map[cg] = element.second;
		cg.clean();
	}
/*
	void canonical_aggregate(QpMapFreq qp_map, CgMapFreq &cg_map) {
		id_map.clear();
		galois::do_all(galois::iterate(qp_map),
			[&](std::pair<QPattern, Frequency> element) {
				CPattern cg(element.first);
				int qp_id = element.first.get_id(); // get quick pattern id
				int cg_id = cg.get_id(); // get canonical pattern id
				slock.lock();
				id_map.insert(std::make_pair(qp_id, cg_id));
				slock.unlock();
				element.first.clean();
				if (cg_map.find(cg) != cg_map.end()) cg_map[cg] += element.second;
				else cg_map[cg] = element.second;
				cg.clean();
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
			galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("CanonicalAggregation")
		);
	}
//*/
	void canonical_aggregate_each(QPattern qp, Frequency freq, CgMapFreq &cg_map) {
		CPattern cg(qp);
		int qp_id = qp.get_id(); // get quick pattern id
		int cg_id = cg.get_id(); // get canonical pattern id
		slock.lock();
		id_map.insert(std::make_pair(qp_id, cg_id));
		slock.unlock();
		qp.clean();
		if (cg_map.find(cg) != cg_map.end()) cg_map[cg] += freq;
		else cg_map[cg] = freq;
		cg.clean();
	}
	// Construct id_map from quick pattern ID (qp_id) to canonical pattern ID (cg_id)
	void canonical_aggregate(QpMapDomain &qp_map, CgMapDomain& cg_map) {
		id_map.clear();
		galois::do_all(galois::iterate(qp_map),
			[&](std::pair<QPattern, DomainSupport> element) {
				unsigned numDomains = element.first.get_size();
				CPattern cg(element.first);
				int qp_id = element.first.get_id();
				int cg_id = cg.get_id();
				slock.lock();
				id_map.insert(std::make_pair(qp_id, cg_id));
				slock.unlock();
				auto it = cg_map.find(cg);
				if (it == cg_map.end()) {
					cg_map[cg].first.resize(numDomains);
					cg_map[cg].second.resize(numDomains);
					element.first.set_cgid(cg.get_id());
				} else {
					element.first.set_cgid((it->first).get_id());
				}
				VertexPositionEquivalences equivalences;
				element.first.get_equivalences(equivalences);
				//std::cout << equivalences << "\n";
				for (unsigned i = 0; i < numDomains; i ++) {
					if (cg_map[cg].first[i] == 0) {
						unsigned qp_idx = cg.get_quick_pattern_index(i);
						assert(qp_idx >= 0 && qp_idx < numDomains);
						UintSet equ_set = equivalences.get_equivalent_set(qp_idx);
						for (unsigned idx : equ_set) {
							if (element.second.first[idx] == 0) {
								cg_map[cg].first[i] = 0;
								cg_map[cg].second[i].insert(element.second.second[idx].begin(), element.second.second[idx].end());
								if (cg_map[cg].second[i].size() >= threshold) {
									cg_map[cg].first[i] = 1;
									cg_map[cg].second[i].clear();
									break;
								}
							} else {
								cg_map[cg].first[i] = 1;
								cg_map[cg].second[i].clear();
								break;
							}
						}
					}
				}
				cg.clean();
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
			galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("CanonicalAggregation")
		);
	}
	void canonical_aggregate_each(QPattern qp, DomainSupport support, CgMapDomain& cg_map) {
		unsigned numDomains = qp.get_size();
		CPattern cg(qp);
		int qp_id = qp.get_id();
		int cg_id = cg.get_id();
		slock.lock();
		id_map.insert(std::make_pair(qp_id, cg_id));
		slock.unlock();
		auto it = cg_map.find(cg);
		if (it == cg_map.end()) {
			cg_map[cg].first.resize(numDomains);
			cg_map[cg].second.resize(numDomains);
			qp.set_cgid(cg.get_id());
		} else {
			qp.set_cgid((it->first).get_id());
		}
		VertexPositionEquivalences equivalences;
		qp.get_equivalences(equivalences);
		//std::cout << equivalences << "\n";
		for (unsigned i = 0; i < numDomains; i ++) {
			if (cg_map[cg].first[i] == 0) {
				unsigned qp_idx = cg.get_quick_pattern_index(i);
				assert(qp_idx >= 0 && qp_idx < numDomains);
				UintSet equ_set = equivalences.get_equivalent_set(qp_idx);
				for (unsigned idx : equ_set) {
					if (support.first[idx] == 0) {
						cg_map[cg].first[i] = 0;
						cg_map[cg].second[i].insert(support.second[idx].begin(), support.second[idx].end());
						if (cg_map[cg].second[i].size() >= threshold) {
							cg_map[cg].first[i] = 1;
							cg_map[cg].second[i].clear();
							break;
						}
					} else {
						cg_map[cg].first[i] = 1;
						cg_map[cg].second[i].clear();
						break;
					}
				}
			}
		}
		cg.clean();
	}
	inline void merge_qp_map(unsigned num, LocalQpMapFreq &qp_localmap, QpMapFreq &qp_map) {
		for (unsigned i = 0; i < qp_localmap.size(); i++) {
			for (auto element : *qp_localmap.getLocal(i)) {
				if (qp_map.find(element.first) != qp_map.end())
					qp_map[element.first] += element.second;
				else qp_map[element.first] = element.second;
			}
		}
	}
	inline void merge_qp_map(unsigned num_domains, const LocalQpMapDomain &qp_localmap, QpMapDomain &qp_map) {
		for (unsigned i = 0; i < qp_localmap.size(); i++) {
			for (auto element : *qp_localmap.getLocal(i)) {
			//galois::do_all(galois::iterate(qp_localmap.getLocal(i)->begin(), qp_localmap.getLocal(i)->end()),
			//	[&](const std::pair<QPattern,DomainSupport> &element) {
				if (qp_map.find(element.first) == qp_map.end()) {
					qp_map[element.first].first = element.second.first;
					qp_map[element.first].second = element.second.second;
				} else {
					for (unsigned i = 0; i < num_domains; i ++) {
						if (qp_map[element.first].first[i] == 0) { // haven't reach threshold yet
							if (element.second.first[i]) {
								qp_map[element.first].first[i] = 1;
								qp_map[element.first].second[i].clear();
							} else {
								qp_map[element.first].second[i].insert(element.second.second[i].begin(), element.second.second[i].end());
								//element.second.second[i].clear();
								if (qp_map[element.first].second[i].size() >= threshold) {
									qp_map[element.first].first[i] = 1;
									qp_map[element.first].second[i].clear();
								}
							}
						}
					}
				}
			//	},
			//	galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
			//	galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			//	galois::loopname("MergeQuickPatterns")
			//);
			}
		}
	}
	inline void merge_cg_map(unsigned num, LocalCgMapFreq &cg_localmap, CgMapFreq &cg_map) {
		for (unsigned i = 0; i < cg_localmap.size(); i++) {
			for (auto element : *cg_localmap.getLocal(i)) {
				if (cg_map.find(element.first) != cg_map.end())
					cg_map[element.first] += element.second;
				else cg_map[element.first] = element.second;
			}
		}
	}
	inline void merge_cg_map(unsigned num_domains, LocalCgMapDomain &cg_localmap, CgMapDomain &cg_map) {
		for (unsigned i = 0; i < cg_localmap.size(); i++) {
			for (auto element : *cg_localmap.getLocal(i)) {
				if (cg_map.find(element.first) == cg_map.end()) {
					cg_map[element.first].first = element.second.first;
					cg_map[element.first].second = element.second.second;
				} else {
					for (unsigned i = 0; i < num_domains; i ++) {
						if (cg_map[element.first].first[i] == 0) { // haven't reach threshold yet
							if (element.second.first[i]) {
								cg_map[element.first].first[i] = 1;
								cg_map[element.first].second[i].clear();
							} else {
								cg_map[element.first].second[i].insert(element.second.second[i].begin(), element.second.second[i].end());
								element.second.second[i].clear();
								if (cg_map[element.first].second[i].size() >= threshold) {
									cg_map[element.first].first[i] = 1;
									cg_map[element.first].second[i].clear();
								}
							}
						}
					}
				}
			}
		}
	}

	// Filtering for FSM
#ifdef ENABLE_LABEL
	inline void init_filter(EdgeEmbeddingQueue &out_queue) {
		is_frequent_edge.resize(graph->sizeEdges());
		std::fill(is_frequent_edge.begin(), is_frequent_edge.end(), 0);
		galois::do_all(galois::iterate(graph->begin(), graph->end()),
			[&](const GNode& src) {
				auto& src_label = graph->getData(src);
				for (auto e : graph->edges(src)) {
					GNode dst = graph->getEdgeDst(e);
					if(src < dst) {
						auto& dst_label = graph->getData(dst);
						InitPattern key = get_init_pattern(src_label, dst_label);
						if (get_support(init_map[key])) { // check the support of this pattern
							EdgeEmbedding new_emb;
							new_emb.push_back(ElementType(src, 0, src_label));
							new_emb.push_back(ElementType(dst, 0, dst_label));
							out_queue.push_back(new_emb);
							unsigned eid = edge_map[OrderedEdge(dst,src)];
							slock.lock();
							is_frequent_edge[*e] = true;
							is_frequent_edge[eid] = true;
							slock.unlock();
						}
					}
				}
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(), galois::no_conflicts(),
			galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("InitFilter")
		);
		std::cout << "Number of frequent edges: " << count(is_frequent_edge.begin(), is_frequent_edge.end(), true) << "\n";
	}
#endif
	// Check if the pattern of a given embedding is frequent, if yes, insert it to the queue
	inline void filter(EdgeEmbeddingQueue &in_queue, EdgeEmbeddingQueue &out_queue) {
		galois::do_all(galois::iterate(in_queue),
			[&](const EdgeEmbedding &emb) {
				unsigned qp_id = emb.get_qpid();
				unsigned cg_id = id_map.at(qp_id);
				if (domain_support_map.at(cg_id)) out_queue.push_back(emb);
			},
			galois::chunk_size<CHUNK_SIZE>(), galois::steal(), 
			galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
			galois::loopname("Filter")
		);
	}
/*
	// This is the filter used in RStream, slow
	void filter_each(const EdgeEmbedding &emb, const CgMapFreq &cg_map, EdgeEmbeddingQueue &out_queue) {
		// find the quick pattern of this embedding
		QPattern qp(emb);
		// find the pattern (canonical graph) of this embedding
		CPattern cg(qp);
		qp.clean();
		// compare the count of this pattern with the threshold
		// if the pattern is frequent, insert this embedding into the task queue
		if (cg_map.at(cg) >= threshold) out_queue.push_back(emb);
		cg.clean();
	}
	inline void filter_each(const EdgeEmbedding &emb, const CgMapDomain &cg_map, EdgeEmbeddingQueue &out_queue) {
		QPattern qp(emb);
		CPattern cg(qp);
		qp.clean();
		bool is_frequent = std::all_of(cg_map.at(cg).first.begin(), cg_map.at(cg).first.end(), [](bool v) { return v; });
		if (is_frequent) out_queue.push_back(emb);
		cg.clean();
	}
	inline void filter_each(const EdgeEmbedding &emb, const UintMap &id_map, const FreqMap &support_map, EdgeEmbeddingQueue &out_queue) {
		unsigned qp_id = emb.get_qpid();
		unsigned cg_id = id_map.at(qp_id);
		if (support_map.at(cg_id) >= threshold) out_queue.push_back(emb);
	}
	inline void filter_each(const EdgeEmbedding &emb, const UintMap &id_map, const DomainMap &support_map, EdgeEmbeddingQueue &out_queue) {
		unsigned qp_id = emb.get_qpid();
		unsigned cg_id = id_map.at(qp_id);
		if (support_map.at(cg_id)) out_queue.push_back(emb);
	}
//*/
	inline void set_threshold(const unsigned minsup) { threshold = minsup; }
	inline void printout_agg(const CgMapFreq &cg_map) {
		for (auto it = cg_map.begin(); it != cg_map.end(); ++it)
			std::cout << "{" << it->first << " --> " << it->second << std::endl;
	}
	inline void printout_agg(const CgMapDomain &cg_map) {
		BoolVec support(cg_map.size());
		int i = 0;
		for (auto it = cg_map.begin(); it != cg_map.end(); ++it) {
			support[i] = get_support(it->second);
			i ++;
		}
		i = 0;
		for (auto it = cg_map.begin(); it != cg_map.end(); ++it) {
			std::cout << "{" << it->first << " --> " << support[i] << std::endl;
			i ++;
		}
	}
	inline unsigned support_count(const CgMapFreq &cg_map) {
		freq_support_map.clear();
		unsigned count = 0;
		for (auto it = cg_map.begin(); it != cg_map.end(); ++it) {
			unsigned support = it->second;
			freq_support_map.insert(std::make_pair(it->first.get_id(), support));
			if (support >= threshold) count ++;
		}
		return count;
	}
	inline unsigned support_count(const CgMapDomain &cg_map) {
		domain_support_map.clear();
		unsigned count = 0;
		for (auto it = cg_map.begin(); it != cg_map.end(); ++it) {
			bool support = get_support(it->second);
			domain_support_map.insert(std::make_pair(it->first.get_id(), support));
			if (support) count ++;
		}
		return count;
	}
	// construct edge-map for later use. May not be necessary if Galois has this support
	void construct_edgemap() {
		for (auto src : *graph) {
			for (auto e : graph->edges(src)) {
				GNode dst = graph->getEdgeDst(e);
				OrderedEdge edge(src, dst);
				edge_map.insert(std::pair<OrderedEdge, unsigned>(edge, *e));
			}
		}
	}

private:
	unsigned threshold;
	InitMap init_map;
	UintMap id_map;
	FreqMap freq_support_map;
	DomainMap domain_support_map;
	galois::gstl::Map<OrderedEdge, unsigned> edge_map;
	std::set<std::pair<VertexId,VertexId> > freq_edge_set;
	std::vector<bool> is_frequent_edge;
	galois::substrate::SimpleLock slock;

	inline InitPattern get_init_pattern(BYTE src_label, BYTE dst_label) {
		if (src_label <= dst_label) return std::make_pair(src_label, dst_label);
		else return std::make_pair(dst_label, src_label);
	}
	bool is_quick_automorphism(unsigned size, unsigned max_size, const EdgeEmbedding& emb, BYTE history, VertexId src, VertexId dst, BYTE& existed) {
		if (dst <= emb.get_vertex(0)) return true;
		if (dst == emb.get_vertex(1)) return true;
		if (history == 0 && dst < emb.get_vertex(1)) return true;
		if (size == 2) {
		} else if (size == 3) {
			if (history == 0 && emb.get_history(2) == 0 && dst <= emb.get_vertex(2)) return true;
			if (history == 0 && emb.get_history(2) == 1 && dst == emb.get_vertex(2)) return true;
			if (history == 1 && emb.get_history(2) == 1 && dst <= emb.get_vertex(2)) return true;
			if (dst == emb.get_vertex(2)) existed = 1;
			//if (!existed && max_size < 4) return true;
		} else {
			std::cout << "Error: should go to detailed check\n";
		}
		return false;
	}
	bool is_edge_automorphism(unsigned size, unsigned max_size, const EdgeEmbedding& emb, BYTE history, VertexId src, VertexId dst, BYTE& existed, const VertexSet& vertex_set) {
		if (size < 3) return is_quick_automorphism(size, max_size, emb, history, src, dst, existed);
		//check with the first element
		if (dst <= emb.get_vertex(0)) return true;
		if (history == 0 && dst <= emb.get_vertex(1)) return true;
		//check loop edge
		if (dst == emb.get_vertex(emb.get_history(history))) return true;
		if (vertex_set.find(dst) != vertex_set.end()) existed = 1;
		// number of vertices must be smaller than k.
		//if (!existed && vertex_set.size() + 1 > max_size) return true;
		//check to see if there already exists the vertex added; if so, just allow to add edge which is (smaller id -> bigger id)
		if (existed && src > dst) return true;
		std::pair<VertexId, VertexId> added_edge(src, dst);
		for (unsigned index = history + 1; index < emb.size(); ++index) {
			std::pair<VertexId, VertexId> edge;
			edge.first = emb.get_vertex(emb.get_history(index));
			edge.second = emb.get_vertex(index);
			//assert(edge.first != edge.second);
			int cmp = compare(added_edge, edge);
			if(cmp <= 0) return true;
		}
		return false;
	}
	/*
	inline bool edge_existed(const EdgeEmbedding& emb, BYTE history, VertexId src, VertexId dst) {
		std::pair<VertexId, VertexId> added_edge(src, dst);
		for (unsigned i = 1; i < emb.size(); ++i) {
			if (emb.get_vertex(i) == dst && emb.get_vertex(emb.get_history(i)) == src)
				return true;
		}
		return false;
	}
	inline void getEdge(const EdgeEmbedding & emb, unsigned index, std::pair<VertexId, VertexId>& edge) {
		edge.first = emb.get_vertex(emb.get_history(index));
		edge.second = emb.get_vertex(index);
		assert(edge.first != edge.second);
	}
	*/
	inline void swap(std::pair<VertexId, VertexId>& pair) {
		if (pair.first > pair.second) {
			VertexId tmp = pair.first;
			pair.first = pair.second;
			pair.second = tmp;
		}
	}
	inline int compare(std::pair<VertexId, VertexId>& oneEdge, std::pair<VertexId, VertexId>& otherEdge) {
		swap(oneEdge);
		swap(otherEdge);
		if(oneEdge.first == otherEdge.first) return oneEdge.second - otherEdge.second;
		else return oneEdge.first - otherEdge.first;
	}
	// counting the minimal image based support
	inline bool get_support(const DomainSupport &supports) {
		//unsigned numDomains = supports.first.size();
		//bool support = 1;
		//for (unsigned j = 0; j < numDomains; j ++)
		//	if (supports.first[j] == 0) support = 0;
		//return support;
		return std::all_of(supports.first.begin(), supports.first.end(), [](bool v) { return v; });
	}
};

#endif // EDGE_MINER_HPP_
